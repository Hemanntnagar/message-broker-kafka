package com.javastream.exception;

/**
 * Base exception for all JavaStream runtime errors.
 */
public class JavaStreamException extends RuntimeException {

    public JavaStreamException(String message) {
        super(message);
    }

    public JavaStreamException(String message, Throwable cause) {
        super(message, cause);
    }
}
