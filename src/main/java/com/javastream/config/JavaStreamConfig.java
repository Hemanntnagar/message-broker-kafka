package com.javastream.config;

import com.javastream.common.Preconditions;
import com.javastream.security.SecuritySettings;

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
    private final int defaultTopicPartitionCount;
    private final int coordinatorBrokerId;
    private final String coordinatorHost;
    private final int coordinatorReplicationPort;
    private final int replicationListenPort;
    private final int replicationFactor;
    private final long clusterSessionTimeoutMs;
    private final long clusterHeartbeatIntervalMs;
    private final String monitoringHttpHost;
    private final int monitoringHttpPort;
    private final String adminApiToken;
    private final SecuritySettings securitySettings;

    public JavaStreamConfig(
            String environment,
            int brokerId,
            String listenHost,
            int listenPort,
            String dataDirectory,
            boolean clusterEnabled,
            int defaultTopicPartitionCount) {
        this(
                environment,
                brokerId,
                listenHost,
                listenPort,
                dataDirectory,
                clusterEnabled,
                defaultTopicPartitionCount,
                0,
                "127.0.0.1",
                0,
                0,
                1,
                15_000,
                2_000,
                "127.0.0.1",
                8080,
                "",
                SecuritySettings.disabled());
    }

    public JavaStreamConfig(
            String environment,
            int brokerId,
            String listenHost,
            int listenPort,
            String dataDirectory,
            boolean clusterEnabled,
            int defaultTopicPartitionCount,
            int coordinatorBrokerId,
            String coordinatorHost,
            int coordinatorReplicationPort,
            int replicationListenPort,
            int replicationFactor,
            long clusterSessionTimeoutMs,
            long clusterHeartbeatIntervalMs,
            String monitoringHttpHost,
            int monitoringHttpPort,
            String adminApiToken) {
        this(
                environment,
                brokerId,
                listenHost,
                listenPort,
                dataDirectory,
                clusterEnabled,
                defaultTopicPartitionCount,
                coordinatorBrokerId,
                coordinatorHost,
                coordinatorReplicationPort,
                replicationListenPort,
                replicationFactor,
                clusterSessionTimeoutMs,
                clusterHeartbeatIntervalMs,
                monitoringHttpHost,
                monitoringHttpPort,
                adminApiToken,
                SecuritySettings.disabled());
    }

    public JavaStreamConfig(
            String environment,
            int brokerId,
            String listenHost,
            int listenPort,
            String dataDirectory,
            boolean clusterEnabled,
            int defaultTopicPartitionCount,
            int coordinatorBrokerId,
            String coordinatorHost,
            int coordinatorReplicationPort,
            int replicationListenPort,
            int replicationFactor,
            long clusterSessionTimeoutMs,
            long clusterHeartbeatIntervalMs,
            String monitoringHttpHost,
            int monitoringHttpPort,
            String adminApiToken,
            SecuritySettings securitySettings) {
        this.environment = Preconditions.requireNonBlank(environment, "environment");
        this.brokerId = brokerId;
        this.listenHost = Preconditions.requireNonBlank(listenHost, "listenHost");
        if (listenPort < 0) {
            throw new com.javastream.exception.JavaStreamException("listenPort must be non-negative");
        }
        this.listenPort = listenPort;
        this.dataDirectory = Preconditions.requireNonBlank(dataDirectory, "dataDirectory");
        this.clusterEnabled = clusterEnabled;
        this.defaultTopicPartitionCount = Preconditions.requirePositive(defaultTopicPartitionCount, "defaultTopicPartitionCount");
        this.coordinatorBrokerId = Math.max(0, coordinatorBrokerId);
        this.coordinatorHost = Preconditions.requireNonBlank(coordinatorHost, "coordinatorHost");
        this.coordinatorReplicationPort = coordinatorReplicationPort;
        this.replicationListenPort = replicationListenPort;
        this.replicationFactor = Preconditions.requirePositive(replicationFactor, "replicationFactor");
        this.clusterSessionTimeoutMs = Preconditions.requirePositive((int) clusterSessionTimeoutMs, "clusterSessionTimeoutMs");
        this.clusterHeartbeatIntervalMs = Preconditions.requirePositive((int) clusterHeartbeatIntervalMs, "clusterHeartbeatIntervalMs");
        this.monitoringHttpHost = Preconditions.requireNonBlank(monitoringHttpHost, "monitoringHttpHost");
        this.monitoringHttpPort = monitoringHttpPort;
        this.adminApiToken = adminApiToken == null ? "" : adminApiToken;
        this.securitySettings = securitySettings == null ? SecuritySettings.disabled() : securitySettings;
    }

    public int defaultTopicPartitionCount() {
        return defaultTopicPartitionCount;
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

    public int coordinatorBrokerId() {
        return coordinatorBrokerId;
    }

    public String coordinatorHost() {
        return coordinatorHost;
    }

    public int coordinatorReplicationPort() {
        return coordinatorReplicationPort;
    }

    public int replicationListenPort() {
        return replicationListenPort;
    }

    public int replicationFactor() {
        return replicationFactor;
    }

    public long clusterSessionTimeoutMs() {
        return clusterSessionTimeoutMs;
    }

    public long clusterHeartbeatIntervalMs() {
        return clusterHeartbeatIntervalMs;
    }

    public String monitoringHttpHost() {
        return monitoringHttpHost;
    }

    public int monitoringHttpPort() {
        return monitoringHttpPort;
    }

    public String adminApiToken() {
        return adminApiToken;
    }

    public SecuritySettings securitySettings() {
        return securitySettings;
    }

    @Override
    public String toString() {
        return "JavaStreamConfig[env=%s, brokerId=%d, listen=%s:%d, dataDir=%s, cluster=%s, defaultPartitions=%d]"
                .formatted(
                        environment,
                        brokerId,
                        listenHost,
                        listenPort,
                        dataDirectory,
                        clusterEnabled,
                        defaultTopicPartitionCount);
    }
}
