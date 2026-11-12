package com.javastream.server;

import com.javastream.broker.BrokerNode;
import com.javastream.broker.ClusterBrokerNode;
import com.javastream.broker.TcpBrokerNode;
import com.javastream.config.JavaStreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process-level server shell. Networking and broker logic plug in via {@link BrokerNode}.
 */
public final class JavaStreamServer implements ServerLifecycle {

    private static final Logger log = LoggerFactory.getLogger(JavaStreamServer.class);

    private final JavaStreamConfig config;
    private final BrokerNode brokerNode;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public JavaStreamServer(JavaStreamConfig config) {
        this(config, config.clusterEnabled() ? new ClusterBrokerNode(config) : new TcpBrokerNode(config));
    }

    public JavaStreamServer(JavaStreamConfig config, BrokerNode brokerNode) {
        this.config = config;
        this.brokerNode = brokerNode;
    }

    public JavaStreamConfig config() {
        return config;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("JavaStream server already running (brokerId={})", config.brokerId());
            return;
        }
        log.info("Starting JavaStream broker: {}", config);
        brokerNode.start();
        log.info("JavaStream broker started on {}:{}", config.listenHost(), config.listenPort());
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping JavaStream broker (brokerId={})", config.brokerId());
        brokerNode.stop();
        log.info("JavaStream broker stopped");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }
}
