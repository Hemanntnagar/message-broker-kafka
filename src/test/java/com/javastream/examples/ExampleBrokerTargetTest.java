package com.javastream.examples;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExampleBrokerTargetTest {

    @Test
    void defaultsWhenEnvUnset() {
        assertEquals("127.0.0.1", ExampleBrokerTarget.host());
        assertEquals(9092, ExampleBrokerTarget.port());
    }
}
