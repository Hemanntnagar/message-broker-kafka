package com.javastream.storage.log;

import com.javastream.exception.JavaStreamException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

/** Persists topic name → partition count under {@code broker/metadata/topics.catalog}. */
public final class TopicCatalog {

    private final Path catalogFile;

    public TopicCatalog(Path catalogFile) {
        this.catalogFile = catalogFile;
    }

    public synchronized Map<String, Integer> load() {
        if (!Files.isRegularFile(catalogFile)) {
            return Map.of();
        }
        Map<String, Integer> topics = new LinkedHashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(catalogFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split("\\s+");
                if (parts.length != 2) {
                    throw new JavaStreamException("Invalid catalog line: " + line);
                }
                topics.put(parts[0], Integer.parseInt(parts[1]));
            }
        } catch (IOException e) {
            throw new LogStorageException("Failed to read topic catalog", e);
        }
        return Map.copyOf(topics);
    }

    public synchronized void put(String topic, int partitionCount) {
        Map<String, Integer> updated = new LinkedHashMap<>(load());
        updated.put(topic, partitionCount);
        writeAll(updated);
    }

    public synchronized void remove(String topic) {
        Map<String, Integer> updated = new LinkedHashMap<>(load());
        updated.remove(topic);
        writeAll(updated);
    }

    private void writeAll(Map<String, Integer> topics) {
        try {
            Files.createDirectories(catalogFile.getParent());
            try (BufferedWriter writer =
                    Files.newBufferedWriter(
                            catalogFile,
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE)) {
                writer.write("# topic partitionCount\n");
                for (var entry : topics.entrySet()) {
                    writer.write(entry.getKey() + " " + entry.getValue());
                    writer.newLine();
                }
            }
        } catch (IOException e) {
            throw new LogStorageException("Failed to write topic catalog", e);
        }
    }
}
