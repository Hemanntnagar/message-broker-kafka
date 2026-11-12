package com.javastream.broker;

import com.javastream.config.JavaStreamConfig;
import com.javastream.server.BrokerServer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Multi-broker cluster node: client TCP listener, inter-broker replication listener, and
 * {@link com.javastream.cluster.ClusterManager} membership.
 */
public final class ClusterBrokerNode implements BrokerNode {

    private final ClusterRuntime runtime;
    private final BrokerServer brokerServer;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ClusterBrokerNode(JavaStreamConfig config) {
        this.runtime = ClusterRuntime.create(config);
        this.brokerServer =
                new BrokerServer(
                        config, runtime.brokerServices(), java.util.Optional.of(runtime.clusterManager()));
    }

    ClusterBrokerNode(ClusterRuntime runtime, BrokerServer brokerServer) {
        this.runtime = runtime;
        this.brokerServer = brokerServer;
    }

    public ClusterRuntime runtime() {
        return runtime;
    }

    public BrokerServer brokerServer() {
        return brokerServer;
    }

    public BrokerServices brokerServices() {
        return runtime.brokerServices();
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            brokerServer.start();
            runtime.startNetworking(brokerServer.boundPort());
        }
    }

    @Override
    public void stop() {
        if (started.compareAndSet(true, false)) {
            runtime.brokerServices().messageStore().flush();
            brokerServer.stop();
            runtime.close();
            runtime.brokerServices().close();
        }
    }

    @Override
    public boolean isStarted() {
        return started.get() && brokerServer.isRunning();
    }
}
