package io.youscan.elasticsearch;

import java.net.InetAddress;

public class LegacyNetworkUtils {

    private static final InetAddress localAddress;

    static {
        InetAddress localAddressX;
        try {
            localAddressX = InetAddress.getLocalHost();
        } catch (Throwable e) {
            // logger.warn("failed to resolve local host, fallback to loopback", e);
            localAddressX = InetAddress.getLoopbackAddress();
        }
        localAddress = localAddressX;
    }

    public static InetAddress getLocalAddress() {
        return localAddress;
    }
}
