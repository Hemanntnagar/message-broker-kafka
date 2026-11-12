package com.javastream.storage;

import com.javastream.exception.JavaStreamException;

/** Raised when the host cannot allocate more bytes for the durable log (ENOSPC). */
public final class DiskFullException extends JavaStreamException {

    public DiskFullException(String message, Throwable cause) {
        super(message, cause);
    }
}
