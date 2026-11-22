package com.limelight.nvstream.usb;

import android.annotation.TargetApi;
import android.content.Context;
import android.net.ConnectivityManager;
import android.net.LinkProperties;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.net.NetworkRequest;
import android.os.Build;
import android.os.SystemClock;

import com.limelight.LimeLog;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Streams over the headset's USB-C cable instead of Wi-Fi.
 *
 * Horizon OS (and Android 12+ builds that ship USB NCM) can turn the USB port
 * into a network adapter. An app asks for it with an ordinary network request
 * on TRANSPORT_USB, the user approves a system dialog once, and the PC sees a
 * new wired network adapter. The link is IPv6 link-local only, so every
 * address on it carries a scope ("fe80::1%ncm0"); this class owns the request,
 * finds the PC on the link with a small mDNS query for Sunshine's
 * _nvstream._tcp service, and hands out what the HTTP and streaming code need
 * to reach a scoped address: the Network to bind to and a resolved address.
 *
 * Nothing here runs unless the "Wired USB streaming" setting is on, because
 * holding the request takes the USB port's networking away from anything
 * else that wants it.
 *
 * Every entry point checks for Android 12 or returns early (no network is ever
 * set below it), which is what the TargetApi stands for.
 */
@TargetApi(Build.VERSION_CODES.S)
public final class UsbLink {
    public interface Listener {
        /** A host answered on the USB link. The address is scoped, e.g. fe80::1%ncm0. */
        void onUsbHostFound(String scopedAddress);

        /** The cable was pulled or the link went away. Forget the USB addresses. */
        void onUsbLinkLost();
    }

    private static final byte[] MDNS_V6_GROUP = {
            (byte) 0xff, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xfb
    };
    private static final int MDNS_PORT = 5353;
    private static final byte[] NVSTREAM_LABEL = "_nvstream".getBytes(StandardCharsets.US_ASCII);

    // Ask often until the PC answers, then just often enough to notice a
    // second PC or a Sunshine restart.
    private static final int QUERY_INTERVAL_SEARCHING_MS = 2000;
    private static final int QUERY_INTERVAL_FOUND_MS = 10000;
    private static final int RESPONSE_WINDOW_MS = 1200;

    private static final Object lock = new Object();
    private static final Set<Listener> listeners = new CopyOnWriteArraySet<>();
    private static final Set<String> hosts = new LinkedHashSet<>();

    private static ConnectivityManager connMgr;
    private static ConnectivityManager.NetworkCallback callback;
    private static volatile Network network;
    private static volatile String interfaceName;
    private static volatile long downstreamKbps;
    private static Thread discoveryThread;
    private static Network boundProcessNetwork;

    private UsbLink() {}

    /** USB networking needs TRANSPORT_USB, which arrived in Android 12. */
    public static boolean isSupported() {
        return Build.VERSION.SDK_INT >= Build.VERSION_CODES.S;
    }

    /**
     * Asks for the USB network. Call it while an activity is in the foreground:
     * the system only shows the approval dialog then, and drops a background
     * request after 30 seconds. Calling it again while a request is held does
     * nothing.
     */
    public static void start(Context context) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.S) {
            return;
        }

        synchronized (lock) {
            if (callback != null) {
                return;
            }

            connMgr = (ConnectivityManager) context.getApplicationContext()
                    .getSystemService(Context.CONNECTIVITY_SERVICE);
            if (connMgr == null) {
                return;
            }

            NetworkRequest request = new NetworkRequest.Builder()
                    .addTransportType(NetworkCapabilities.TRANSPORT_USB)
                    .removeCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                    .removeCapability(NetworkCapabilities.NET_CAPABILITY_TRUSTED)
                    .build();

            callback = new ConnectivityManager.NetworkCallback() {
                @Override
                public void onAvailable(Network net) {
                    LimeLog.info("USB link: network available");
                    network = net;
                    LinkProperties lp = connMgr.getLinkProperties(net);
                    if (lp != null && lp.getInterfaceName() != null) {
                        setInterface(net, lp.getInterfaceName());
                    }
                }

                @Override
                public void onLinkPropertiesChanged(Network net, LinkProperties lp) {
                    if (lp.getInterfaceName() != null) {
                        setInterface(net, lp.getInterfaceName());
                    }
                }

                @Override
                public void onCapabilitiesChanged(Network net, NetworkCapabilities caps) {
                    long kbps = caps.getLinkDownstreamBandwidthKbps();
                    if (kbps != downstreamKbps) {
                        downstreamKbps = kbps;
                        // About 480,000 on a USB 2 cable and 5,120,000 on USB 3
                        LimeLog.info("USB link: " + (kbps / 1000) + " Mbps ("
                                + (kbps >= 1000000 ? "USB 3" : "USB 2") + ")");
                    }
                }

                @Override
                public void onLost(Network net) {
                    LimeLog.info("USB link: lost");
                    linkGone(net);
                }

                @Override
                public void onUnavailable() {
                    // Declined, or asked from the background. The request is
                    // already released, so the next start() asks afresh.
                    LimeLog.info("USB link: not granted");
                    synchronized (lock) {
                        callback = null;
                    }
                }
            };

            try {
                connMgr.requestNetwork(request, callback);
                LimeLog.info("USB link: requested");
            } catch (RuntimeException e) {
                // SecurityException on builds without the permission, or
                // IllegalArgumentException where the transport is unknown
                LimeLog.warning("USB link: request failed: " + e);
                callback = null;
            }
        }
    }

    /** Releases the USB network so other apps can use the port. */
    public static void stop() {
        synchronized (lock) {
            if (callback == null) {
                return;
            }
            try {
                connMgr.unregisterNetworkCallback(callback);
            } catch (RuntimeException ignored) {
            }
            callback = null;
        }
        Network net = network;
        if (net != null) {
            linkGone(net);
        }
    }

    public static void addListener(Listener listener) {
        listeners.add(listener);

        // Tell a late listener about hosts that are already known
        List<String> known;
        synchronized (lock) {
            known = new ArrayList<>(hosts);
        }
        for (String host : known) {
            listener.onUsbHostFound(host);
        }
    }

    public static void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    public static boolean isUp() {
        return network != null && interfaceName != null;
    }

    public static Network getNetwork() {
        return network;
    }

    public static String getInterfaceName() {
        return interfaceName;
    }

    /** The scoped addresses of every host found on the link so far. */
    public static List<String> getHosts() {
        synchronized (lock) {
            return new ArrayList<>(hosts);
        }
    }

    /**
     * Whether an address carries an IPv6 scope, which is what every address on
     * the USB link looks like. OkHttp and Java URLs cannot hold a scope, so
     * these need the special handling in NvHTTP.
     */
    public static boolean isScopedAddress(String address) {
        return address != null && address.indexOf(':') >= 0 && address.indexOf('%') > 0;
    }

    /** Whether an address is on the USB link that is up right now. */
    public static boolean isUsbAddress(String address) {
        String iface = interfaceName;
        if (!isScopedAddress(address) || iface == null) {
            return false;
        }
        return address.substring(address.indexOf('%') + 1).equals(iface);
    }

    /**
     * Adds the USB link's scope to a bare link-local address, which is what
     * Windows shows in ipconfig without its own "%12" suffix. Anything else
     * comes back unchanged.
     */
    public static String scopeIfLinkLocal(String address) {
        String iface = interfaceName;
        if (iface == null || address == null || address.indexOf('%') >= 0) {
            return address;
        }
        String lower = address.toLowerCase(Locale.ROOT);
        if (lower.startsWith("fe8") || lower.startsWith("fe9")
                || lower.startsWith("fea") || lower.startsWith("feb")) {
            return address + "%" + iface;
        }
        return address;
    }

    /** Turns "fe80::1%ncm0" into an Inet6Address with the scope set. */
    public static InetAddress resolve(String scopedAddress) throws UnknownHostException {
        int pct = scopedAddress.indexOf('%');
        if (pct < 0) {
            return InetAddress.getByName(scopedAddress);
        }

        String literal = scopedAddress.substring(0, pct);
        String scope = scopedAddress.substring(pct + 1);
        byte[] bytes = InetAddress.getByName(literal).getAddress();

        try {
            NetworkInterface ni = NetworkInterface.getByName(scope);
            if (ni != null) {
                return Inet6Address.getByAddress(null, bytes, ni);
            }
            // A numeric scope, as Windows writes them
            return Inet6Address.getByAddress(null, bytes, Integer.parseInt(scope));
        } catch (IOException | NumberFormatException e) {
            // Let the platform parser have a go as a last resort
            return InetAddress.getByName(scopedAddress);
        }
    }

    /**
     * Sends all of the process's traffic over the USB link, which is how the
     * native streaming core's sockets end up on it. Call when a stream to a
     * USB address starts and undo with unbindProcess() when it ends.
     */
    public static boolean bindProcess() {
        synchronized (lock) {
            Network net = network;
            if (net == null || connMgr == null) {
                return false;
            }
            if (connMgr.bindProcessToNetwork(net)) {
                boundProcessNetwork = net;
                LimeLog.info("USB link: process bound for streaming");
                return true;
            }
            return false;
        }
    }

    public static void unbindProcess() {
        synchronized (lock) {
            if (boundProcessNetwork != null && connMgr != null) {
                connMgr.bindProcessToNetwork(null);
                boundProcessNetwork = null;
                LimeLog.info("USB link: process unbound");
            }
        }
    }

    private static void setInterface(Network net, String iface) {
        synchronized (lock) {
            if (net != network) {
                return;
            }
            if (!iface.equals(interfaceName)) {
                LimeLog.info("USB link: interface " + iface);
                interfaceName = iface;
                hosts.clear();
            }
            if (discoveryThread == null) {
                discoveryThread = new Thread(() -> discoveryLoop(net), "USB link discovery");
                discoveryThread.setDaemon(true);
                discoveryThread.start();
            }
        }
    }

    private static void linkGone(Network net) {
        synchronized (lock) {
            if (net != network) {
                return;
            }
            network = null;
            interfaceName = null;
            hosts.clear();
            if (discoveryThread != null) {
                discoveryThread.interrupt();
                discoveryThread = null;
            }
            if (boundProcessNetwork == net) {
                connMgr.bindProcessToNetwork(null);
                boundProcessNetwork = null;
            }
        }
        for (Listener l : listeners) {
            l.onUsbLinkLost();
        }
    }

    private static void discoveryLoop(Network net) {
        while (!Thread.currentThread().isInterrupted() && network == net) {
            String iface = interfaceName;
            if (iface != null) {
                try {
                    queryOnce(net, iface);
                } catch (IOException e) {
                    LimeLog.info("USB link: mDNS query failed: " + e);
                }
            }

            boolean found;
            synchronized (lock) {
                found = !hosts.isEmpty();
            }
            try {
                Thread.sleep(found ? QUERY_INTERVAL_FOUND_MS : QUERY_INTERVAL_SEARCHING_MS);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    /**
     * Asks the link for _nvstream._tcp.local and takes the source address of
     * every answer as a host. The query goes from an ordinary port, so the PC's
     * mDNS responder answers straight back to it (a "legacy unicast" reply)
     * and nothing has to join the multicast group.
     */
    private static void queryOnce(Network net, String iface) throws IOException {
        NetworkInterface ni = NetworkInterface.getByName(iface);
        if (ni == null) {
            return;
        }
        Inet6Address group = Inet6Address.getByAddress(null, MDNS_V6_GROUP, ni);

        try (MulticastSocket sock = new MulticastSocket(0)) {
            net.bindSocket(sock);
            sock.setNetworkInterface(ni);
            sock.setTimeToLive(255);
            sock.setLoopbackMode(true); // true disables loopback

            byte[] query = buildPtrQuery();
            sock.send(new DatagramPacket(query, query.length, group, MDNS_PORT));

            byte[] buf = new byte[9000];
            long deadline = SystemClock.elapsedRealtime() + RESPONSE_WINDOW_MS;
            while (true) {
                long left = deadline - SystemClock.elapsedRealtime();
                if (left <= 0) {
                    break;
                }
                sock.setSoTimeout((int) left);
                DatagramPacket pkt = new DatagramPacket(buf, buf.length);
                try {
                    sock.receive(pkt);
                } catch (SocketTimeoutException e) {
                    break;
                }

                if (!isNvstreamAnswer(pkt.getData(), pkt.getLength())) {
                    continue;
                }
                InetAddress src = pkt.getAddress();
                if (!(src instanceof Inet6Address) || !src.isLinkLocalAddress()) {
                    continue;
                }

                String literal = src.getHostAddress();
                int pct = literal.indexOf('%');
                if (pct >= 0) {
                    literal = literal.substring(0, pct);
                }
                foundHost(net, literal + "%" + iface);
            }
        }
    }

    private static void foundHost(Network net, String scoped) {
        synchronized (lock) {
            if (net != network || !hosts.add(scoped)) {
                return;
            }
        }
        LimeLog.info("USB link: found host " + scoped);
        for (Listener l : listeners) {
            l.onUsbHostFound(scoped);
        }
    }

    // A one-question PTR query for _nvstream._tcp.local with the
    // unicast-response bit set
    private static byte[] buildPtrQuery() {
        String[] labels = {"_nvstream", "_tcp", "local"};
        int len = 12 + 1 + 4;
        for (String l : labels) {
            len += 1 + l.length();
        }
        byte[] q = new byte[len];
        int i = 0;
        // Header: id 0, flags 0, one question, nothing else
        q[i++] = 0; q[i++] = 0;
        q[i++] = 0; q[i++] = 0;
        q[i++] = 0; q[i++] = 1;
        q[i++] = 0; q[i++] = 0;
        q[i++] = 0; q[i++] = 0;
        q[i++] = 0; q[i++] = 0;
        for (String l : labels) {
            q[i++] = (byte) l.length();
            for (char c : l.toCharArray()) {
                q[i++] = (byte) c;
            }
        }
        q[i++] = 0;
        q[i++] = 0; q[i++] = 12;            // PTR
        q[i++] = (byte) 0x80; q[i] = 1;     // QU, class IN
        return q;
    }

    // A response (QR set) with at least one answer that mentions _nvstream.
    // Good enough to find the PC; NvHTTP checks it really is a host after.
    private static boolean isNvstreamAnswer(byte[] data, int len) {
        if (len < 12 || (data[2] & 0x80) == 0) {
            return false;
        }
        int answers = ((data[6] & 0xff) << 8) | (data[7] & 0xff);
        int additional = ((data[10] & 0xff) << 8) | (data[11] & 0xff);
        if (answers == 0 && additional == 0) {
            return false;
        }
        outer:
        for (int i = 12; i + NVSTREAM_LABEL.length + 1 <= len; i++) {
            if (data[i] != NVSTREAM_LABEL.length) {
                continue;
            }
            for (int j = 0; j < NVSTREAM_LABEL.length; j++) {
                byte b = data[i + 1 + j];
                if (b >= 'A' && b <= 'Z') {
                    b += 'a' - 'A';
                }
                if (b != NVSTREAM_LABEL[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }
}
