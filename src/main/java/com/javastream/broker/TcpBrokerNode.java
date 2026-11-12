package com.javastream.broker;

import com.javastream.config.JavaStreamConfig;
import com.javastream.server.BrokerServer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Broker node that exposes the TCP {@link BrokerServer} acceptor for Phase 1 networking.
 */
public final class TcpBrokerNode implements BrokerNode {

    private final BrokerServer brokerServer;
    private final BrokerServices brokerServices;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public TcpBrokerNode(JavaStreamConfig config) {
        BrokerServices services = new BrokerServices(config);
        this.brokerServices = services;
        this.brokerServer = new BrokerServer(config, services, java.util.Optional.empty());
    }

    TcpBrokerNode(BrokerServices brokerServices, BrokerServer brokerServer) {
        this.brokerServices = brokerServices;
        this.brokerServer = brokerServer;
    }

    TcpBrokerNode(BrokerServer brokerServer) {
        this.brokerServices = new BrokerServices();
        this.brokerServer = brokerServer;
    }

    public BrokerServices brokerServices() {
        return brokerServices;
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
            brokerServices.close();
        }
    }

    @Override
    public boolean isStarted() {
        return started.get() && brokerServer.isRunning();
    }
}
