package com.javastream.consumer;

import com.javastream.exception.JavaStreamException;

/** Signals that message processing may be retried without committing the offset. */
public class RetryableProcessingException extends JavaStreamException {

    public RetryableProcessingException(String message) {
        super(message);
    }

    public RetryableProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
