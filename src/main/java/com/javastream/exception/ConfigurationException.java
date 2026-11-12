package com.javastream.exception;

/**
 * Raised when configuration is invalid or cannot be loaded.
 */
public final class ConfigurationException extends JavaStreamException {

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
