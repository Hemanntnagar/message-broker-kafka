package com.javastream.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigLoaderTest {

    @Test
    void loadDefaultsFromClasspath() {
        JavaStreamConfig config = ConfigLoader.load();
        assertEquals("development", config.environment());
        assertEquals(0, config.brokerId());
        assertEquals("0.0.0.0", config.listenHost());
        assertEquals(9092, config.listenPort());
        assertEquals("./data", config.dataDirectory());
        assertFalse(config.clusterEnabled());
        assertEquals(1, config.defaultTopicPartitionCount());
    }

    @Test
    void loadOverridesFromFile(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve("broker.properties");
        Files.writeString(file, """
                broker.id=3
                network.listen.port=19092
                cluster.enabled=true
                """);

        JavaStreamConfig config = ConfigLoader.load(file);
        assertEquals(3, config.brokerId());
        assertEquals(19092, config.listenPort());
        assertTrue(config.clusterEnabled());
    }
}
