package com.javastream.delivery;

import com.javastream.exception.JavaStreamException;

/** Raised when an idempotent producer sends an invalid or out-of-order sequence number. */
public final class IdempotentSequenceException extends JavaStreamException {

    public IdempotentSequenceException(String message) {
        super(message);
    }
}
