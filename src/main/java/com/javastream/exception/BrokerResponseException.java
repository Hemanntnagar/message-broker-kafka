package com.javastream.exception;

import com.javastream.protocol.StatusCode;

/**
 * Broker returned a non-OK status for a client request.
 */
public final class BrokerResponseException extends JavaStreamException {

    private final StatusCode status;

    public BrokerResponseException(StatusCode status, String message) {
        super(message);
        this.status = status;
    }

    public StatusCode status() {
        return status;
    }
}
