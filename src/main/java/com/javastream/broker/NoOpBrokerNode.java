package com.javastream.broker;

import com.javastream.config.JavaStreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Placeholder broker node for Phase 0 wiring and tests.
 */
public final class NoOpBrokerNode implements BrokerNode {

    private static final Logger log = LoggerFactory.getLogger(NoOpBrokerNode.class);

    private final JavaStreamConfig config;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public NoOpBrokerNode(JavaStreamConfig config) {
        this.config = config;
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            log.debug("NoOpBrokerNode started for brokerId={}", config.brokerId());
        }
    }

    @Override
    public void stop() {
        started.set(false);
    }

    @Override
    public boolean isStarted() {
        return started.get();
    }
}
