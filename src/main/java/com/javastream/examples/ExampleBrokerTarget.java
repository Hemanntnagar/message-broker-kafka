package com.javastream.examples;

/**
 * Broker address for runnable examples (defaults: {@code 127.0.0.1:9092}).
 *
 * <p>Override with {@code JAVASTREAM_EXAMPLE_BROKER_HOST} and {@code JAVASTREAM_EXAMPLE_BROKER_PORT}
 * when the broker runs in Docker or on another host.
 */
public final class ExampleBrokerTarget {

    private ExampleBrokerTarget() {}

    public static String host() {
        String fromEnv = System.getenv("JAVASTREAM_EXAMPLE_BROKER_HOST");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv.trim();
        }
        return "127.0.0.1";
    }

    public static int port() {
        String fromEnv = System.getenv("JAVASTREAM_EXAMPLE_BROKER_PORT");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return Integer.parseInt(fromEnv.trim());
        }
        return 9092;
    }
}
