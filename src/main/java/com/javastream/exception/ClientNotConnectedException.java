package com.javastream.exception;

/**
 * Thrown when a producer or consumer client is used while disconnected.
 */
public final class ClientNotConnectedException extends JavaStreamException {

    public ClientNotConnectedException(String message) {
        super(message);
    }
}
