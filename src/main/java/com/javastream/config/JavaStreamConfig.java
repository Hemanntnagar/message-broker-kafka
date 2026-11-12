package com.javastream.config;

import com.javastream.common.Preconditions;

/**
 * Immutable runtime configuration for a single broker instance.
 */
public final class JavaStreamConfig {

    private final String environment;
    private final int brokerId;
    private final String listenHost;
    private final int listenPort;
    private final String dataDirectory;
    private final boolean clusterEnabled;

    public JavaStreamConfig(
            String environment,
            int brokerId,
            String listenHost,
            int listenPort,
            String dataDirectory,
            boolean clusterEnabled) {
        this.environment = Preconditions.requireNonBlank(environment, "environment");
        this.brokerId = brokerId;
        this.listenHost = Preconditions.requireNonBlank(listenHost, "listenHost");
        this.listenPort = Preconditions.requirePositive(listenPort, "listenPort");
        this.dataDirectory = Preconditions.requireNonBlank(dataDirectory, "dataDirectory");
        this.clusterEnabled = clusterEnabled;
    }

    public String environment() {
        return environment;
    }

    public int brokerId() {
        return brokerId;
    }

    public String listenHost() {
        return listenHost;
    }

    public int listenPort() {
        return listenPort;
    }

    public String dataDirectory() {
        return dataDirectory;
    }

    public boolean clusterEnabled() {
        return clusterEnabled;
    }

    @Override
    public String toString() {
        return "JavaStreamConfig[env=%s, brokerId=%d, listen=%s:%d, dataDir=%s, cluster=%s]"
                .formatted(environment, brokerId, listenHost, listenPort, dataDirectory, clusterEnabled);
    }
}
