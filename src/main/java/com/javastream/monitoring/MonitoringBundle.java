package com.javastream.monitoring;

import com.javastream.admin.AdminAuthority;
import com.javastream.admin.BrokerAdminService;
import com.javastream.broker.BrokerServices;
import com.javastream.cluster.ClusterManager;
import com.javastream.config.JavaStreamConfig;
import com.javastream.metrics.BrokerMetrics;
import com.javastream.server.BrokerServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/** Wires Micrometer metrics, admin service, and optional HTTP monitoring endpoints. */
public final class MonitoringBundle implements AutoCloseable {

    private final BrokerMetrics metrics;
    private final BrokerAdminService adminService;
    private final AdminAuthority adminAuthority;
    private final MonitoringHttpServer httpServer;
    private AtomicBoolean ready = new AtomicBoolean(false);

    private MonitoringBundle(
            BrokerMetrics metrics,
            BrokerAdminService adminService,
            AdminAuthority adminAuthority,
            MonitoringHttpServer httpServer) {
        this.metrics = metrics;
        this.adminService = adminService;
        this.adminAuthority = adminAuthority;
        this.httpServer = httpServer;
    }

    public static MonitoringBundle create(
            JavaStreamConfig config,
            BrokerServices services,
            Optional<ClusterManager> clusterManager,
            int boundClientPort) {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Instant startedAt = Instant.now();
        BrokerMetrics metrics = new BrokerMetrics(registry, config.brokerId(), startedAt);
        metrics.attach(services, Path.of(config.dataDirectory()), clusterManager);
        AdminAuthority authority = AdminAuthority.fromConfig(config.adminApiToken());
        BrokerAdminService adminService =
                new BrokerAdminService(
                        services,
                        clusterManager,
                        startedAt,
                        config.listenHost(),
                        boundClientPort > 0 ? boundClientPort : config.listenPort());
        AtomicBoolean readyFlag = new AtomicBoolean(false);
        int monitoringPort =
                config.monitoringHttpPort() <= 0
                        ? 0
                        : config.monitoringHttpPort() + config.brokerId();
        MonitoringHttpServer httpServer =
                new MonitoringHttpServer(
                        config.monitoringHttpHost(),
                        monitoringPort,
                        metrics,
                        adminService,
                        authority,
                        readyFlag::get);
        MonitoringBundle bundle = new MonitoringBundle(metrics, adminService, authority, httpServer);
        bundle.ready = readyFlag;
        return bundle;
    }

    public BrokerMetrics metrics() {
        return metrics;
    }

    public BrokerAdminService adminService() {
        return adminService;
    }

    public AdminAuthority adminAuthority() {
        return adminAuthority;
    }

    public void attach(BrokerServer brokerServer) {
        metrics.setActiveConnections(brokerServer::activeConnectionCount);
        ready.set(brokerServer.isRunning());
    }

    public void startHttp() {
        httpServer.start();
    }

    public void stopHttp() {
        httpServer.close();
    }

  public void setReady(boolean value) {
        ready.set(value);
    }

    @Override
    public void close() {
        httpServer.close();
        metrics.close();
    }
}
