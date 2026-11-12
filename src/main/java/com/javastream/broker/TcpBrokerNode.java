package com.javastream.broker;

import com.javastream.config.JavaStreamConfig;
import com.javastream.server.BrokerServer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Broker node that exposes the TCP {@link BrokerServer} acceptor for Phase 1 networking.
 */
public final class TcpBrokerNode implements BrokerNode {

    private final BrokerServer brokerServer;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public TcpBrokerNode(JavaStreamConfig config) {
        this.brokerServer = new BrokerServer(config);
    }

    TcpBrokerNode(BrokerServer brokerServer) {
        this.brokerServer = brokerServer;
    }

    public BrokerServer brokerServer() {
        return brokerServer;
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            brokerServer.start();
        }
    }

    @Override
    public void stop() {
        if (started.compareAndSet(true, false)) {
            brokerServer.stop();
        }
    }

    @Override
    public boolean isStarted() {
        return started.get() && brokerServer.isRunning();
    }
}
