package com.javastream.server;

/**
 * Lifecycle contract for long-running broker processes.
 */
public interface ServerLifecycle {

    void start();

    void stop();

    boolean isRunning();
}
