package com.javastream.common;

import com.javastream.exception.JavaStreamException;

/**
 * Lightweight argument checks without external dependencies.
 */
public final class Preconditions {

    private Preconditions() {
    }

    public static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new JavaStreamException(name + " must be non-blank");
        }
        return value;
    }

    public static int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new JavaStreamException(name + " must be positive");
        }
        return value;
    }
}
