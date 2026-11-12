package com.javastream.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EnvironmentConfigSourceTest {

    @Test
    void mapsEnvPrefixToPropertyKeys() {
        assertEquals("network.listen.port",
                EnvironmentConfigSource.envKeyToPropertyKey("JAVASTREAM_NETWORK_LISTEN_PORT"));
        assertEquals("broker.id",
                EnvironmentConfigSource.envKeyToPropertyKey("JAVASTREAM_BROKER_ID"));
    }
}
