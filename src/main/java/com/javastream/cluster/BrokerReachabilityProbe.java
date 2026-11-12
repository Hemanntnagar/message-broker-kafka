package com.javastream.cluster;

import java.net.InetSocketAddress;
import java.net.Socket;

/** Best-effort TCP connect probe; used only after heartbeats lapse (never as the sole liveness signal). */
public final class BrokerReachabilityProbe {

    private final int connectTimeoutMs;

    public BrokerReachabilityProbe(int connectTimeoutMs) {
        this.connectTimeoutMs = Math.max(100, connectTimeoutMs);
    }

    public boolean canConnect(BrokerEndpoint endpoint) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(endpoint.host(), endpoint.replicationPort()), connectTimeoutMs);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }
}
