package com.javastream.storage.log;

import com.javastream.partition.PartitionId;
import com.javastream.topic.TopicName;

import java.nio.file.Path;

/** Resolves on-disk paths under {@code data/broker/}. */
public final class StorageLayout {

    private final Path brokerRoot;

    public StorageLayout(Path brokerRoot) {
        this.brokerRoot = brokerRoot;
    }

    public Path metadataDirectory() {
        return brokerRoot.resolve("metadata");
    }

    public Path topicCatalogFile() {
        return metadataDirectory().resolve("topics.catalog");
    }

    public Path partitionDirectory(PartitionId partitionId) {
        TopicName topic = partitionId.topic();
        return brokerRoot
                .resolve("topics")
                .resolve(topic.value())
                .resolve("partition-" + partitionId.index());
    }

    public Path segmentLogFile(Path partitionDir, int baseOffset) {
        return partitionDir.resolve("segment-%06d.log".formatted(baseOffset));
    }

    public Path segmentIndexFile(Path partitionDir, int baseOffset) {
        return partitionDir.resolve("segment-%06d.index".formatted(baseOffset));
    }

    public Path brokerRoot() {
        return brokerRoot;
    }
}
