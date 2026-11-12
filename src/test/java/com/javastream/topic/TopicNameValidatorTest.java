package com.javastream.topic;

import com.javastream.exception.JavaStreamException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TopicNameValidatorTest {

    @Test
    void acceptsValidNames() {
        assertEquals("orders.v1", TopicNameValidator.validate("orders.v1"));
        assertEquals("a-b_c", TopicNameValidator.validate("a-b_c"));
    }

    @Test
    void rejectsInvalidCharacters() {
        assertThrows(JavaStreamException.class, () -> TopicNameValidator.validate("bad name"));
        assertThrows(JavaStreamException.class, () -> TopicNameValidator.validate("slash/here"));
    }
}
