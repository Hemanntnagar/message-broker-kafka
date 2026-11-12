package com.javastream.server;

import com.javastream.broker.NoOpBrokerNode;
import com.javastream.config.JavaStreamConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JavaStreamServerTest {

    @Test
    void startAndStopToggleRunningState() {
        JavaStreamConfig config = new JavaStreamConfig(
                "test", 1, "127.0.0.1", 19092, "./data-test", false);
        JavaStreamServer server = new JavaStreamServer(config, new NoOpBrokerNode(config));

        assertFalse(server.isRunning());
        server.start();
        assertTrue(server.isRunning());
        server.stop();
        assertFalse(server.isRunning());
    }

    @Test
    void doubleStartIsIdempotent() {
        JavaStreamConfig config = new JavaStreamConfig(
                "test", 1, "127.0.0.1", 19092, "./data-test", false);
        JavaStreamServer server = new JavaStreamServer(config, new NoOpBrokerNode(config));
        server.start();
        server.start();
        assertTrue(server.isRunning());
        server.stop();
    }
}
