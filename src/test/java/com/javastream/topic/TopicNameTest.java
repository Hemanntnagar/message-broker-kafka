package com.javastream.topic;

import com.javastream.exception.JavaStreamException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TopicNameTest {

    @Test
    void exposesValue() {
        TopicName name = new TopicName("orders");
        assertEquals("orders", name.value());
        assertEquals("orders", name.toString());
    }

    @Test
    void rejectsBlankName() {
        assertThrows(JavaStreamException.class, () -> new TopicName("  "));
    }
}
