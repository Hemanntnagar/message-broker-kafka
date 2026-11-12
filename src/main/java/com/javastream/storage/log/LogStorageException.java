package com.javastream.storage.log;

import com.javastream.exception.JavaStreamException;

/** Raised when durable log I/O or integrity checks fail. */
public final class LogStorageException extends JavaStreamException {

    public LogStorageException(String message) {
        super(message);
    }

    public LogStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
