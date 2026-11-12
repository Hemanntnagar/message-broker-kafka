package com.javastream.protocol;

import com.javastream.delivery.ProducerAckLevel;
import com.javastream.exception.ProtocolDecodeException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Encode/decode produce, fetch, and ack operation payloads (v1). */
public final class RecordPayloadCodec {

    public static final int AUTO_PARTITION = -1;

    private RecordPayloadCodec() {}

    public record ProduceRecord(byte[] key, byte[] value, Map<String, byte[]> headers) {}

    public record ProduceRequest(
            String topic,
            int partitionHint,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers,
            ProducerAckLevel ackLevel,
            int requestTimeoutMillis,
            long producerId,
            long sequenceNumber,
            List<ProduceRecord> additionalRecords) {

        public ProduceRequest(String topic, int partitionHint, byte[] key, byte[] value, Map<String, byte[]> headers) {
            this(
                    topic,
                    partitionHint,
                    key,
                    value,
                    headers,
                    ProducerAckLevel.ACK_LEADER,
                    0,
                    -1,
                    0,
                    List.of());
        }

        public List<ProduceRecord> allRecords() {
            if (additionalRecords == null || additionalRecords.isEmpty()) {
                return List.of(new ProduceRecord(key, value, headers));
            }
            List<ProduceRecord> all = new ArrayList<>(1 + additionalRecords.size());
            all.add(new ProduceRecord(key, value, headers));
            all.addAll(additionalRecords);
            return List.copyOf(all);
        }
    }

    private static final int PRODUCE_DELIVERY_EXTENSION_BYTES = 1 + 4 + 8 + 8;

    public record ProduceResponse(long messageId, int partition, long offset) {}

    public record ProduceBatchResponse(List<ProduceResponse> responses) {}

    public record FetchRequest(String topic, int partition, long offset, int maxRecords, int timeoutMillis) {}

    public record FetchedRecord(
            long messageId,
            int partition,
            long offset,
            long timestampEpochMillis,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers) {}

    public record FetchResponse(List<FetchedRecord> records) {}

    public record AckRequest(String topic, int partition, long offset) {}

    public record CreateTopicRequest(String topic, int partitionCount) {}

    public record ListTopicsResponse(List<TopicEntry> topics) {}

    public record TopicEntry(String name, int partitionCount) {}

    public record GetMetadataRequest(String topic) {}

    public record DeleteTopicRequest(String topic) {}

    public record JoinGroupRequest(String groupId, String memberId, String topic) {}

    public record JoinGroupResponse(
            int generation,
            List<Integer> assignedPartitions,
            List<Integer> revokedPartitions,
            List<CommittedOffsetEntry> committedOffsets) {}

    public record HeartbeatRequest(String groupId, String memberId) {}

    public record LeaveGroupRequest(String groupId, String memberId) {}

    public record CommitOffsetsRequest(String groupId, String topic, List<CommittedOffsetEntry> offsets) {}

    public record CommittedOffsetEntry(int partition, long offset) {}

    public record FetchCommittedOffsetsRequest(String groupId, String topic, List<Integer> partitions) {}

    public record FetchCommittedOffsetsResponse(List<CommittedOffsetEntry> offsets) {}

    public static byte[] encodeProduceRequest(ProduceRequest request) {
        byte[] topicBytes = utf8(request.topic());
        List<ProduceRecord> trailing = request.additionalRecords() == null ? List.of() : request.additionalRecords();
        int trailingSize = trailingRecordBytes(trailing);
        ByteBuffer buffer =
                ByteBuffer.allocate(
                                2
                                        + topicBytes.length
                                        + 4
                                        + 4
                                        + nullableLength(request.key())
                                        + 4
                                        + nullableLength(request.value())
                                        + 2
                                        + headerBytes(request.headers())
                                        + PRODUCE_DELIVERY_EXTENSION_BYTES
                                        + (trailing.isEmpty() ? 0 : 4 + trailingSize))
                        .order(ByteOrder.BIG_ENDIAN);
        putUtf8Topic(buffer, topicBytes);
        buffer.putInt(request.partitionHint());
        putNullableBytes(buffer, request.key());
        putNullableBytes(buffer, request.value());
        putHeaders(buffer, request.headers());
        buffer.put(request.ackLevel().wireCode());
        buffer.putInt(request.requestTimeoutMillis());
        buffer.putLong(request.producerId());
        buffer.putLong(request.sequenceNumber());
        if (!trailing.isEmpty()) {
            buffer.putInt(trailing.size());
            for (ProduceRecord record : trailing) {
                putProduceRecordBody(buffer, record);
            }
        }
        return toByteArray(buffer);
    }

    public static ProduceRequest decodeProduceRequest(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String topic = readUtf8Topic(buffer);
        int partitionHint = buffer.getInt();
        byte[] key = readNullableBytes(buffer);
        byte[] value = readNullableBytes(buffer);
        Map<String, byte[]> headers = readHeaders(buffer);
        ProducerAckLevel ackLevel = ProducerAckLevel.ACK_LEADER;
        int requestTimeoutMillis = 0;
        long producerId = -1;
        long sequenceNumber = 0;
        if (buffer.remaining() >= PRODUCE_DELIVERY_EXTENSION_BYTES) {
            ackLevel = ProducerAckLevel.fromWire(buffer.get());
            requestTimeoutMillis = buffer.getInt();
            producerId = buffer.getLong();
            sequenceNumber = buffer.getLong();
        }
        List<ProduceRecord> additional = List.of();
        if (buffer.remaining() >= 4) {
            int additionalCount = buffer.getInt();
            if (additionalCount < 0) {
                throw new ProtocolDecodeException("Negative produce batch count: " + additionalCount);
            }
            if (additionalCount > 0) {
                List<ProduceRecord> records = new ArrayList<>(additionalCount);
                for (int i = 0; i < additionalCount; i++) {
                    records.add(readProduceRecordBody(buffer));
                }
                additional = List.copyOf(records);
            }
        }
        ensureConsumed(buffer);
        return new ProduceRequest(
                topic,
                partitionHint,
                key,
                value,
                headers,
                ackLevel,
                requestTimeoutMillis,
                producerId,
                sequenceNumber,
                additional);
    }

    public static byte[] encodeProduceResponse(ProduceResponse response) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + 8).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(response.messageId());
        buffer.putInt(response.partition());
        buffer.putLong(response.offset());
        return toByteArray(buffer);
    }

    public static byte[] encodeProduceBatchResponse(ProduceBatchResponse response) {
        List<ProduceResponse> list = response.responses();
        if (list.size() == 1) {
            return encodeProduceResponse(list.getFirst());
        }
        ByteBuffer buffer = ByteBuffer.allocate(4 + list.size() * (8 + 4 + 8)).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(list.size());
        for (ProduceResponse item : list) {
            buffer.putLong(item.messageId());
            buffer.putInt(item.partition());
            buffer.putLong(item.offset());
        }
        return toByteArray(buffer);
    }

    public static ProduceBatchResponse decodeProduceResponse(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        if (buffer.remaining() == 8 + 4 + 8) {
            return new ProduceBatchResponse(List.of(decodeSingleProduceResponse(buffer)));
        }
        if (buffer.remaining() < 4) {
            throw new ProtocolDecodeException("Truncated produce response");
        }
        int count = buffer.getInt();
        if (count <= 0) {
            throw new ProtocolDecodeException("Invalid produce batch response count: " + count);
        }
        List<ProduceResponse> responses = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            responses.add(decodeSingleProduceResponse(buffer));
        }
        ensureConsumed(buffer);
        return new ProduceBatchResponse(List.copyOf(responses));
    }

    private static ProduceResponse decodeSingleProduceResponse(ByteBuffer buffer) {
        if (buffer.remaining() < 8 + 4 + 8) {
            throw new ProtocolDecodeException("Truncated produce response entry");
        }
        return new ProduceResponse(buffer.getLong(), buffer.getInt(), buffer.getLong());
    }

    private static void putProduceRecordBody(ByteBuffer buffer, ProduceRecord record) {
        putNullableBytes(buffer, record.key());
        putNullableBytes(buffer, record.value());
        putHeaders(buffer, record.headers());
    }

    private static ProduceRecord readProduceRecordBody(ByteBuffer buffer) {
        byte[] key = readNullableBytes(buffer);
        byte[] value = readNullableBytes(buffer);
        Map<String, byte[]> headers = readHeaders(buffer);
        return new ProduceRecord(key, value, headers);
    }

    private static int trailingRecordBytes(List<ProduceRecord> records) {
        int size = 0;
        for (ProduceRecord record : records) {
            size += 4 + nullableLength(record.key()) + 4 + nullableLength(record.value()) + headerBytes(record.headers());
        }
        return size;
    }

    public static byte[] encodeFetchRequest(FetchRequest request) {
        byte[] topicBytes = utf8(request.topic());
        ByteBuffer buffer = ByteBuffer.allocate(2 + topicBytes.length + 4 + 8 + 4 + 4).order(ByteOrder.BIG_ENDIAN);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(request.partition());
        buffer.putLong(request.offset());
        buffer.putInt(request.maxRecords());
        buffer.putInt(request.timeoutMillis());
        return toByteArray(buffer);
    }

    public static FetchRequest decodeFetchRequest(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String topic = readUtf8Topic(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxRecords = buffer.getInt();
        int timeoutMillis = buffer.getInt();
        ensureConsumed(buffer);
        return new FetchRequest(topic, partition, offset, maxRecords, timeoutMillis);
    }

    public static byte[] encodeFetchResponse(FetchResponse response) {
        int size = 4;
        for (FetchedRecord record : response.records()) {
            size += recordSize(record);
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(response.records().size());
        for (FetchedRecord record : response.records()) {
            writeRecord(buffer, record);
        }
        return toByteArray(buffer);
    }

    public static FetchResponse decodeFetchResponse(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int count = buffer.getInt();
        if (count < 0) {
            throw new ProtocolDecodeException("Negative fetch record count: " + count);
        }
        List<FetchedRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            records.add(readRecord(buffer));
        }
        ensureConsumed(buffer);
        return new FetchResponse(List.copyOf(records));
    }

    public static byte[] encodeAckRequest(AckRequest request) {
        byte[] topicBytes = utf8(request.topic());
        ByteBuffer buffer = ByteBuffer.allocate(2 + topicBytes.length + 4 + 8).order(ByteOrder.BIG_ENDIAN);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(request.partition());
        buffer.putLong(request.offset());
        return toByteArray(buffer);
    }

    public static AckRequest decodeAckRequest(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String topic = readUtf8Topic(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        ensureConsumed(buffer);
        return new AckRequest(topic, partition, offset);
    }

    public static byte[] encodeCreateTopicRequest(CreateTopicRequest request) {
        byte[] topicBytes = utf8(request.topic());
        ByteBuffer buffer = ByteBuffer.allocate(2 + topicBytes.length + 4).order(ByteOrder.BIG_ENDIAN);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(request.partitionCount());
        return toByteArray(buffer);
    }

    public static CreateTopicRequest decodeCreateTopicRequest(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String topic = readUtf8Topic(buffer);
        int partitionCount = buffer.getInt();
        ensureConsumed(buffer);
        return new CreateTopicRequest(topic, partitionCount);
    }

    public static byte[] encodeListTopicsResponse(ListTopicsResponse response) {
        int size = 4;
        for (TopicEntry entry : response.topics()) {
            byte[] nameBytes = utf8(entry.name());
            size += 2 + nameBytes.length + 4;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(response.topics().size());
        for (TopicEntry entry : response.topics()) {
            byte[] nameBytes = utf8(entry.name());
            buffer.putShort((short) nameBytes.length);
            buffer.put(nameBytes);
            buffer.putInt(entry.partitionCount());
        }
        return toByteArray(buffer);
    }

    public static ListTopicsResponse decodeListTopicsResponse(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int count = buffer.getInt();
        if (count < 0) {
            throw new ProtocolDecodeException("Negative topic count: " + count);
        }
        List<TopicEntry> topics = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String name = readUtf8Topic(buffer);
            int partitionCount = buffer.getInt();
            topics.add(new TopicEntry(name, partitionCount));
        }
        ensureConsumed(buffer);
        return new ListTopicsResponse(List.copyOf(topics));
    }

    public static byte[] encodeGetMetadataRequest(GetMetadataRequest request) {
        byte[] topicBytes = utf8(request.topic());
        ByteBuffer buffer = ByteBuffer.allocate(2 + topicBytes.length).order(ByteOrder.BIG_ENDIAN);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        return toByteArray(buffer);
    }

    public static GetMetadataRequest decodeGetMetadataRequest(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String topic = readUtf8Topic(buffer);
        ensureConsumed(buffer);
        return new GetMetadataRequest(topic);
    }

    public static byte[] encodeGetMetadataResponse(com.javastream.topic.TopicMetadata metadata) {
        int size = 2 + utf8(metadata.topicName()).length + 4 + 4;
        for (var partition : metadata.partitions()) {
            size += 4 + 8 + 8;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        byte[] topicBytes = utf8(metadata.topicName());
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(metadata.partitionCount());
        buffer.putInt(metadata.partitions().size());
        for (var partition : metadata.partitions()) {
            buffer.putInt(partition.partitionIndex());
            buffer.putLong(partition.logEndOffset());
            buffer.putLong(partition.recordCount());
        }
        return toByteArray(buffer);
    }

    public static com.javastream.topic.TopicMetadata decodeGetMetadataResponse(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String topic = readUtf8Topic(buffer);
        int partitionCount = buffer.getInt();
        int listed = buffer.getInt();
        List<com.javastream.partition.PartitionMetadata> partitions = new ArrayList<>(listed);
        for (int i = 0; i < listed; i++) {
            int index = buffer.getInt();
            long logEnd = buffer.getLong();
            long records = buffer.getLong();
            partitions.add(new com.javastream.partition.PartitionMetadata(index, logEnd, records));
        }
        ensureConsumed(buffer);
        return new com.javastream.topic.TopicMetadata(topic, partitionCount, partitions);
    }

    public static byte[] encodeDeleteTopicRequest(DeleteTopicRequest request) {
        byte[] topicBytes = utf8(request.topic());
        ByteBuffer buffer = ByteBuffer.allocate(2 + topicBytes.length).order(ByteOrder.BIG_ENDIAN);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        return toByteArray(buffer);
    }

    public static DeleteTopicRequest decodeDeleteTopicRequest(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String topic = readUtf8Topic(buffer);
        ensureConsumed(buffer);
        return new DeleteTopicRequest(topic);
    }

    public static byte[] encodeJoinGroupRequest(JoinGroupRequest request) {
        byte[] groupBytes = utf8(request.groupId());
        byte[] memberBytes = utf8(request.memberId());
        byte[] topicBytes = utf8(request.topic());
        ByteBuffer buffer =
                ByteBuffer.allocate(2 + groupBytes.length + 2 + memberBytes.length + 2 + topicBytes.length)
                        .order(ByteOrder.BIG_ENDIAN);
        putShortUtf8(buffer, groupBytes);
        putShortUtf8(buffer, memberBytes);
        putShortUtf8(buffer, topicBytes);
        return toByteArray(buffer);
    }

    public static JoinGroupRequest decodeJoinGroupRequest(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String groupId = readShortUtf8(buffer, "group id");
        String memberId = readShortUtf8(buffer, "member id");
        String topic = readUtf8Topic(buffer);
        ensureConsumed(buffer);
        return new JoinGroupRequest(groupId, memberId, topic);
    }

    public static byte[] encodeJoinGroupResponse(JoinGroupResponse response) {
        int size = 4 + 4 + 4 * response.assignedPartitions().size() + 4 + 4 * response.revokedPartitions().size();
        size += 4 + response.committedOffsets().size() * (4 + 8);
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(response.generation());
        putIntList(buffer, response.assignedPartitions());
        putIntList(buffer, response.revokedPartitions());
        putOffsetEntries(buffer, response.committedOffsets());
        return toByteArray(buffer);
    }

    public static JoinGroupResponse decodeJoinGroupResponse(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int generation = buffer.getInt();
        List<Integer> assigned = readIntList(buffer);
        List<Integer> revoked = readIntList(buffer);
        List<CommittedOffsetEntry> committed = readOffsetEntries(buffer);
        ensureConsumed(buffer);
        return new JoinGroupResponse(generation, assigned, revoked, committed);
    }

    public static byte[] encodeHeartbeatRequest(HeartbeatRequest request) {
        byte[] groupBytes = utf8(request.groupId());
        byte[] memberBytes = utf8(request.memberId());
        ByteBuffer buffer =
                ByteBuffer.allocate(2 + groupBytes.length + 2 + memberBytes.length).order(ByteOrder.BIG_ENDIAN);
        putShortUtf8(buffer, groupBytes);
        putShortUtf8(buffer, memberBytes);
        return toByteArray(buffer);
    }

    public static HeartbeatRequest decodeHeartbeatRequest(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String groupId = readShortUtf8(buffer, "group id");
        String memberId = readShortUtf8(buffer, "member id");
        ensureConsumed(buffer);
        return new HeartbeatRequest(groupId, memberId);
    }

    public static byte[] encodeLeaveGroupRequest(LeaveGroupRequest request) {
        return encodeHeartbeatRequest(new HeartbeatRequest(request.groupId(), request.memberId()));
    }

    public static LeaveGroupRequest decodeLeaveGroupRequest(byte[] payload) {
        HeartbeatRequest heartbeat = decodeHeartbeatRequest(payload);
        return new LeaveGroupRequest(heartbeat.groupId(), heartbeat.memberId());
    }

    public static byte[] encodeCommitOffsetsRequest(CommitOffsetsRequest request) {
        byte[] groupBytes = utf8(request.groupId());
        byte[] topicBytes = utf8(request.topic());
        int size = 2 + groupBytes.length + 2 + topicBytes.length + 4 + request.offsets().size() * (4 + 8);
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        putShortUtf8(buffer, groupBytes);
        putUtf8Topic(buffer, topicBytes);
        putOffsetEntries(buffer, request.offsets());
        return toByteArray(buffer);
    }

    public static CommitOffsetsRequest decodeCommitOffsetsRequest(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String groupId = readShortUtf8(buffer, "group id");
        String topic = readUtf8Topic(buffer);
        List<CommittedOffsetEntry> offsets = readOffsetEntries(buffer);
        ensureConsumed(buffer);
        return new CommitOffsetsRequest(groupId, topic, offsets);
    }

    public static byte[] encodeFetchCommittedOffsetsRequest(FetchCommittedOffsetsRequest request) {
        byte[] groupBytes = utf8(request.groupId());
        byte[] topicBytes = utf8(request.topic());
        int size = 2 + groupBytes.length + 2 + topicBytes.length + 4 + 4 * request.partitions().size();
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        putShortUtf8(buffer, groupBytes);
        putUtf8Topic(buffer, topicBytes);
        putIntList(buffer, request.partitions());
        return toByteArray(buffer);
    }

    public static FetchCommittedOffsetsRequest decodeFetchCommittedOffsetsRequest(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String groupId = readShortUtf8(buffer, "group id");
        String topic = readUtf8Topic(buffer);
        List<Integer> partitions = readIntList(buffer);
        ensureConsumed(buffer);
        return new FetchCommittedOffsetsRequest(groupId, topic, partitions);
    }

    public static byte[] encodeFetchCommittedOffsetsResponse(FetchCommittedOffsetsResponse response) {
        ByteBuffer buffer =
                ByteBuffer.allocate(4 + response.offsets().size() * (4 + 8)).order(ByteOrder.BIG_ENDIAN);
        putOffsetEntries(buffer, response.offsets());
        return toByteArray(buffer);
    }

    public static FetchCommittedOffsetsResponse decodeFetchCommittedOffsetsResponse(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        List<CommittedOffsetEntry> offsets = readOffsetEntries(buffer);
        ensureConsumed(buffer);
        return new FetchCommittedOffsetsResponse(offsets);
    }

    private static void putShortUtf8(ByteBuffer buffer, byte[] bytes) {
        if (bytes.length > Short.MAX_VALUE) {
            throw new ProtocolDecodeException("UTF-8 field too long");
        }
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    private static void putIntList(ByteBuffer buffer, List<Integer> values) {
        buffer.putInt(values.size());
        for (int value : values) {
            buffer.putInt(value);
        }
    }

    private static List<Integer> readIntList(ByteBuffer buffer) {
        if (buffer.remaining() < 4) {
            throw new ProtocolDecodeException("Truncated int list length");
        }
        int count = buffer.getInt();
        if (count < 0) {
            throw new ProtocolDecodeException("Negative int list count: " + count);
        }
        List<Integer> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(buffer.getInt());
        }
        return List.copyOf(values);
    }

    private static void putOffsetEntries(ByteBuffer buffer, List<CommittedOffsetEntry> entries) {
        buffer.putInt(entries.size());
        for (CommittedOffsetEntry entry : entries) {
            buffer.putInt(entry.partition());
            buffer.putLong(entry.offset());
        }
    }

    private static List<CommittedOffsetEntry> readOffsetEntries(ByteBuffer buffer) {
        if (buffer.remaining() < 4) {
            throw new ProtocolDecodeException("Truncated offset entry count");
        }
        int count = buffer.getInt();
        if (count < 0) {
            throw new ProtocolDecodeException("Negative offset entry count: " + count);
        }
        List<CommittedOffsetEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(new CommittedOffsetEntry(buffer.getInt(), buffer.getLong()));
        }
        return List.copyOf(entries);
    }

    private static int recordSize(FetchedRecord record) {
        return 8 + 4 + 8 + 8 + 4 + nullableLength(record.key()) + 4 + nullableLength(record.value()) + 2
                + headerBytes(record.headers());
    }

    private static void writeRecord(ByteBuffer buffer, FetchedRecord record) {
        buffer.putLong(record.messageId());
        buffer.putInt(record.partition());
        buffer.putLong(record.offset());
        buffer.putLong(record.timestampEpochMillis());
        putNullableBytes(buffer, record.key());
        putNullableBytes(buffer, record.value());
        putHeaders(buffer, record.headers());
    }

    private static FetchedRecord readRecord(ByteBuffer buffer) {
        long messageId = buffer.getLong();
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        long timestamp = buffer.getLong();
        byte[] key = readNullableBytes(buffer);
        byte[] value = readNullableBytes(buffer);
        Map<String, byte[]> headers = readHeaders(buffer);
        return new FetchedRecord(messageId, partition, offset, timestamp, key, value, headers);
    }

    private static void putUtf8Topic(ByteBuffer buffer, byte[] topicBytes) {
        if (topicBytes.length > Short.MAX_VALUE) {
            throw new ProtocolDecodeException("Topic name too long");
        }
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
    }

    private static String readUtf8Topic(ByteBuffer buffer) {
        if (buffer.remaining() < 2) {
            throw new ProtocolDecodeException("Truncated topic length");
        }
        int length = buffer.getShort() & 0xFFFF;
        if (buffer.remaining() < length) {
            throw new ProtocolDecodeException("Truncated topic name");
        }
        byte[] topicBytes = new byte[length];
        buffer.get(topicBytes);
        return new String(topicBytes, StandardCharsets.UTF_8);
    }

    private static void putNullableBytes(ByteBuffer buffer, byte[] bytes) {
        if (bytes == null) {
            buffer.putInt(-1);
            return;
        }
        if (bytes.length > Integer.MAX_VALUE - 4) {
            throw new ProtocolDecodeException("Byte array too large");
        }
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    private static byte[] readNullableBytes(ByteBuffer buffer) {
        if (buffer.remaining() < 4) {
            throw new ProtocolDecodeException("Truncated nullable length");
        }
        int length = buffer.getInt();
        if (length < 0) {
            return null;
        }
        if (buffer.remaining() < length) {
            throw new ProtocolDecodeException("Truncated byte payload");
        }
        byte[] value = new byte[length];
        buffer.get(value);
        return value;
    }

    private static int nullableLength(byte[] bytes) {
        return bytes == null ? 4 : 4 + bytes.length;
    }

    private static int headerBytes(Map<String, byte[]> headers) {
        int size = 2;
        if (headers == null || headers.isEmpty()) {
            return size;
        }
        for (Map.Entry<String, byte[]> entry : headers.entrySet()) {
            byte[] key = utf8(entry.getKey());
            byte[] value = entry.getValue() == null ? new byte[0] : entry.getValue();
            size += 2 + key.length + 2 + value.length;
        }
        return size;
    }

    private static void putHeaders(ByteBuffer buffer, Map<String, byte[]> headers) {
        if (headers == null || headers.isEmpty()) {
            buffer.putShort((short) 0);
            return;
        }
        if (headers.size() > Short.MAX_VALUE) {
            throw new ProtocolDecodeException("Too many headers");
        }
        buffer.putShort((short) headers.size());
        for (Map.Entry<String, byte[]> entry : headers.entrySet()) {
            byte[] key = utf8(entry.getKey());
            byte[] value = entry.getValue() == null ? new byte[0] : entry.getValue();
            if (key.length > Short.MAX_VALUE || value.length > Short.MAX_VALUE) {
                throw new ProtocolDecodeException("Header entry too large");
            }
            buffer.putShort((short) key.length);
            buffer.put(key);
            buffer.putShort((short) value.length);
            buffer.put(value);
        }
    }

    private static Map<String, byte[]> readHeaders(ByteBuffer buffer) {
        if (buffer.remaining() < 2) {
            throw new ProtocolDecodeException("Truncated header count");
        }
        int count = buffer.getShort() & 0xFFFF;
        if (count == 0) {
            return Map.of();
        }
        Map<String, byte[]> headers = new HashMap<>();
        for (int i = 0; i < count; i++) {
            headers.put(readShortUtf8(buffer, "header key"), readShortBytes(buffer, "header value"));
        }
        return Map.copyOf(headers);
    }

    private static String readShortUtf8(ByteBuffer buffer, String field) {
        byte[] bytes = readShortBytes(buffer, field);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] readShortBytes(ByteBuffer buffer, String field) {
        if (buffer.remaining() < 2) {
            throw new ProtocolDecodeException("Truncated " + field + " length");
        }
        int length = buffer.getShort() & 0xFFFF;
        if (buffer.remaining() < length) {
            throw new ProtocolDecodeException("Truncated " + field);
        }
        byte[] value = new byte[length];
        buffer.get(value);
        return value;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void ensureConsumed(ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            throw new ProtocolDecodeException("Trailing bytes in payload (" + buffer.remaining() + " left)");
        }
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] out = new byte[buffer.position()];
        buffer.flip();
        buffer.get(out);
        return out;
    }
}
