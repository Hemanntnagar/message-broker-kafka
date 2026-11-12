package com.javastream.cluster;

/**
 * Network location of a broker in the cluster (client and inter-broker replication ports).
 */
public record BrokerEndpoint(int brokerId, String host, int clientPort, int replicationPort) {

    public BrokerEndpoint {
        if (brokerId < 0) {
            throw new IllegalArgumentException("brokerId must be non-negative");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must be non-blank");
        }
        if (clientPort <= 0 || replicationPort <= 0) {
            throw new IllegalArgumentException("ports must be positive");
        }
    }

    public BrokerId id() {
        return new BrokerId(brokerId);
    }
}
