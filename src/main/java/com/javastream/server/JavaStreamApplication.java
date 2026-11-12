package com.javastream.server;

import com.javastream.config.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Primary entry point for a JavaStream broker instance.
 */
public final class JavaStreamApplication {

    private static final Logger log = LoggerFactory.getLogger(JavaStreamApplication.class);

    private JavaStreamApplication() {
    }

    public static void main(String[] args) {
        log.info("JavaStream broker bootstrap (TCP + JSTR protocol v1)");
        JavaStreamServer server = new JavaStreamServer(ConfigLoader.load());
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "javastream-shutdown"));
        server.start();
        awaitTermination();
    }

    private static void awaitTermination() {
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Main thread interrupted");
        }
    }
}
