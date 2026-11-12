package com.javastream.common;

import com.javastream.exception.JavaStreamException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PreconditionsTest {

    @Test
    void requireNonBlankAcceptsValidString() {
        assertEquals("ok", Preconditions.requireNonBlank("ok", "field"));
    }

    @Test
    void requirePositiveRejectsZero() {
        assertThrows(JavaStreamException.class, () -> Preconditions.requirePositive(0, "port"));
    }
}
