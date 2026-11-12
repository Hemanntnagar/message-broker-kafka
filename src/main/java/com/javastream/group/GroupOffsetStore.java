package com.javastream.group;

import com.javastream.exception.JavaStreamException;
import com.javastream.storage.log.LogStorageException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

/** Persists committed offsets under {@code metadata/consumer-offsets.catalog}. */
public final class GroupOffsetStore {

    private final Path catalogFile;
    private final boolean durable;

    public GroupOffsetStore(Path catalogFile) {
        this.catalogFile = catalogFile;
        this.durable = catalogFile != null;
    }

    public Map<GroupPartitionOffsetKey, Long> load() {
        if (!durable || !Files.isRegularFile(catalogFile)) {
            return Map.of();
        }
        Map<GroupPartitionOffsetKey, Long> offsets = new LinkedHashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(catalogFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split("\\s+");
                if (parts.length != 4) {
                    throw new JavaStreamException("Invalid consumer offset line: " + line);
                }
                GroupPartitionOffsetKey key =
                        new GroupPartitionOffsetKey(parts[0], parts[1], Integer.parseInt(parts[2]));
                offsets.put(key, Long.parseLong(parts[3]));
            }
        } catch (IOException e) {
            throw new LogStorageException("Failed to read consumer offsets catalog", e);
        }
        return Map.copyOf(offsets);
    }

    public synchronized void persist(Map<GroupPartitionOffsetKey, Long> offsets) {
        if (!durable) {
            return;
        }
        try {
            Files.createDirectories(catalogFile.getParent());
            try (BufferedWriter writer =
                    Files.newBufferedWriter(
                            catalogFile,
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE)) {
                writer.write("# groupId topic partition offset\n");
                for (var entry : offsets.entrySet()) {
                    GroupPartitionOffsetKey key = entry.getKey();
                    writer.write(
                            key.groupId() + " " + key.topic() + " " + key.partition() + " " + entry.getValue());
                    writer.newLine();
                }
            }
        } catch (IOException e) {
            throw new LogStorageException("Failed to write consumer offsets catalog", e);
        }
    }
}
