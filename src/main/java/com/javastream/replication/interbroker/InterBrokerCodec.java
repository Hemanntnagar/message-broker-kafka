package com.javastream.replication.interbroker;

import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.storage.BrokerMessageStore;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Payload encoding for inter-broker RPCs. */
public final class InterBrokerCodec {

    public record RegisterRequest(int brokerId, String host, int clientPort, int replicationPort) {}

    public record HeartbeatRequest(int brokerId) {}

    public record ReplicateRecordRequest(
            String topic,
            int partition,
            long messageId,
            long offset,
            long timestampEpochMillis,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers) {}

    public record ReplicateRecordResponse(long replicaLogEndOffset) {}

    public record ReplicaFetchRequest(String topic, int partition, long fromOffset, int maxRecords) {}

    public record TopicCreatedNotification(String topic, int partitionCount) {}

    public record PartitionAssignmentEntry(String topic, int partition, int leaderId, List<Integer> replicaIds) {}

    public record BrokerDirectoryEntry(int brokerId, String host, int clientPort, int replicationPort) {}

    public record AssignmentPush(List<BrokerDirectoryEntry> brokers, List<PartitionAssignmentEntry> assignments) {}

    private InterBrokerCodec() {}

    public static byte[] encodeRegister(RegisterRequest request) {
        byte[] hostBytes = request.host().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer =
                ByteBuffer.allocate(4 + 2 + hostBytes.length + 4 + 4)
                        .order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(request.brokerId());
        buffer.putShort((short) hostBytes.length);
        buffer.put(hostBytes);
        buffer.putInt(request.clientPort());
        buffer.putInt(request.replicationPort());
        return buffer.array();
    }

    public static RegisterRequest decodeRegister(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int brokerId = buffer.getInt();
        int hostLen = buffer.getShort() & 0xFFFF;
        byte[] hostBytes = new byte[hostLen];
        buffer.get(hostBytes);
        int clientPort = buffer.getInt();
        int replicationPort = buffer.getInt();
        return new RegisterRequest(brokerId, new String(hostBytes, StandardCharsets.UTF_8), clientPort, replicationPort);
    }

    public static byte[] encodeHeartbeat(HeartbeatRequest request) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(request.brokerId()).array();
    }

    public static HeartbeatRequest decodeHeartbeat(byte[] payload) {
        return new HeartbeatRequest(ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN).getInt());
    }

    public static byte[] encodeReplicate(ReplicateRecordRequest request) {
        byte[] topicBytes = request.topic().getBytes(StandardCharsets.UTF_8);
        int headerSize = headerBytes(request.headers());
        ByteBuffer buffer =
                ByteBuffer.allocate(
                                2
                                        + topicBytes.length
                                        + 4
                                        + 8
                                        + 8
                                        + 8
                                        + 4
                                        + nullableLen(request.key())
                                        + 4
                                        + nullableLen(request.value())
                                        + 2
                                        + headerSize)
                        .order(ByteOrder.BIG_ENDIAN);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(request.partition());
        buffer.putLong(request.messageId());
        buffer.putLong(request.offset());
        buffer.putLong(request.timestampEpochMillis());
        putNullable(buffer, request.key());
        putNullable(buffer, request.value());
        putHeaders(buffer, request.headers());
        return buffer.array();
    }

    public static ReplicateRecordRequest decodeReplicate(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int topicLen = buffer.getShort() & 0xFFFF;
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        int partition = buffer.getInt();
        long messageId = buffer.getLong();
        long offset = buffer.getLong();
        long timestamp = buffer.getLong();
        byte[] key = readNullable(buffer);
        byte[] value = readNullable(buffer);
        Map<String, byte[]> headers = readHeaders(buffer);
        return new ReplicateRecordRequest(
                new String(topicBytes, StandardCharsets.UTF_8),
                partition,
                messageId,
                offset,
                timestamp,
                key,
                value,
                headers);
    }

    public static byte[] encodeReplicateResponse(ReplicateRecordResponse response) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(response.replicaLogEndOffset()).array();
    }

    public static ReplicateRecordResponse decodeReplicateResponse(byte[] payload) {
        return new ReplicateRecordResponse(ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN).getLong());
    }

    public static byte[] encodeReplicaFetch(ReplicaFetchRequest request) {
        byte[] topicBytes = request.topic().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer =
                ByteBuffer.allocate(2 + topicBytes.length + 4 + 8 + 4).order(ByteOrder.BIG_ENDIAN);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(request.partition());
        buffer.putLong(request.fromOffset());
        buffer.putInt(request.maxRecords());
        return buffer.array();
    }

    public static ReplicaFetchRequest decodeReplicaFetch(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int topicLen = buffer.getShort() & 0xFFFF;
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        int partition = buffer.getInt();
        long fromOffset = buffer.getLong();
        int maxRecords = buffer.getInt();
        return new ReplicaFetchRequest(new String(topicBytes, StandardCharsets.UTF_8), partition, fromOffset, maxRecords);
    }

    public static byte[] encodeReplicaFetchResponse(List<BrokerMessageStore.StoredRecord> records) {
        List<RecordPayloadCodec.FetchedRecord> fetched = new ArrayList<>(records.size());
        for (BrokerMessageStore.StoredRecord record : records) {
            fetched.add(
                    new RecordPayloadCodec.FetchedRecord(
                            record.messageId(),
                            0,
                            record.offset(),
                            record.timestampEpochMillis(),
                            record.key(),
                            record.value(),
                            record.headers()));
        }
        return RecordPayloadCodec.encodeFetchResponse(new RecordPayloadCodec.FetchResponse(fetched));
    }

    public static List<BrokerMessageStore.StoredRecord> decodeReplicaFetchResponse(byte[] payload) {
        RecordPayloadCodec.FetchResponse response = RecordPayloadCodec.decodeFetchResponse(payload);
        List<BrokerMessageStore.StoredRecord> records = new ArrayList<>(response.records().size());
        for (RecordPayloadCodec.FetchedRecord record : response.records()) {
            records.add(
                    new BrokerMessageStore.StoredRecord(
                            record.messageId(),
                            record.offset(),
                            record.timestampEpochMillis(),
                            record.key(),
                            record.value(),
                            record.headers()));
        }
        return records;
    }

    public static byte[] encodeTopicCreated(TopicCreatedNotification notification) {
        byte[] topicBytes = notification.topic().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(2 + topicBytes.length + 4).order(ByteOrder.BIG_ENDIAN);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(notification.partitionCount());
        return buffer.array();
    }

    public static TopicCreatedNotification decodeTopicCreated(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int topicLen = buffer.getShort() & 0xFFFF;
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        int partitionCount = buffer.getInt();
        return new TopicCreatedNotification(new String(topicBytes, StandardCharsets.UTF_8), partitionCount);
    }

    public static byte[] encodeAssignmentPush(AssignmentPush push) {
        int size = 4;
        for (BrokerDirectoryEntry broker : push.brokers()) {
            byte[] hostBytes = broker.host().getBytes(StandardCharsets.UTF_8);
            size += 4 + 2 + hostBytes.length + 4 + 4;
        }
        size += 4;
        for (PartitionAssignmentEntry entry : push.assignments()) {
            byte[] topicBytes = entry.topic().getBytes(StandardCharsets.UTF_8);
            size += 2 + topicBytes.length + 4 + 4 + 4 + entry.replicaIds().size() * 4;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(push.brokers().size());
        for (BrokerDirectoryEntry broker : push.brokers()) {
            byte[] hostBytes = broker.host().getBytes(StandardCharsets.UTF_8);
            buffer.putInt(broker.brokerId());
            buffer.putShort((short) hostBytes.length);
            buffer.put(hostBytes);
            buffer.putInt(broker.clientPort());
            buffer.putInt(broker.replicationPort());
        }
        buffer.putInt(push.assignments().size());
        for (PartitionAssignmentEntry entry : push.assignments()) {
            byte[] topicBytes = entry.topic().getBytes(StandardCharsets.UTF_8);
            buffer.putShort((short) topicBytes.length);
            buffer.put(topicBytes);
            buffer.putInt(entry.partition());
            buffer.putInt(entry.leaderId());
            buffer.putInt(entry.replicaIds().size());
            for (int replicaId : entry.replicaIds()) {
                buffer.putInt(replicaId);
            }
        }
        return buffer.array();
    }

    public static AssignmentPush decodeAssignmentPush(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int brokerCount = buffer.getInt();
        List<BrokerDirectoryEntry> brokers = new ArrayList<>(brokerCount);
        for (int i = 0; i < brokerCount; i++) {
            int brokerId = buffer.getInt();
            int hostLen = buffer.getShort() & 0xFFFF;
            byte[] hostBytes = new byte[hostLen];
            buffer.get(hostBytes);
            int clientPort = buffer.getInt();
            int replicationPort = buffer.getInt();
            brokers.add(
                    new BrokerDirectoryEntry(
                            brokerId, new String(hostBytes, StandardCharsets.UTF_8), clientPort, replicationPort));
        }
        int count = buffer.getInt();
        List<PartitionAssignmentEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int topicLen = buffer.getShort() & 0xFFFF;
            byte[] topicBytes = new byte[topicLen];
            buffer.get(topicBytes);
            int partition = buffer.getInt();
            int leaderId = buffer.getInt();
            int replicaCount = buffer.getInt();
            List<Integer> replicas = new ArrayList<>(replicaCount);
            for (int r = 0; r < replicaCount; r++) {
                replicas.add(buffer.getInt());
            }
            entries.add(
                    new PartitionAssignmentEntry(
                            new String(topicBytes, StandardCharsets.UTF_8), partition, leaderId, replicas));
        }
        return new AssignmentPush(List.copyOf(brokers), List.copyOf(entries));
    }

    private static int nullableLen(byte[] bytes) {
        return bytes == null ? 0 : bytes.length;
    }

    private static void putNullable(ByteBuffer buffer, byte[] bytes) {
        if (bytes == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        }
    }

    private static byte[] readNullable(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    private static int headerBytes(Map<String, byte[]> headers) {
        int size = 0;
        for (Map.Entry<String, byte[]> entry : headers.entrySet()) {
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] value = entry.getValue();
            size += 2 + key.length + 2 + (value == null ? 0 : value.length);
        }
        return size;
    }

    private static void putHeaders(ByteBuffer buffer, Map<String, byte[]> headers) {
        buffer.putShort((short) headers.size());
        for (Map.Entry<String, byte[]> entry : headers.entrySet()) {
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] value = entry.getValue();
            buffer.putShort((short) key.length);
            buffer.put(key);
            if (value == null) {
                buffer.putShort((short) 0);
            } else {
                buffer.putShort((short) value.length);
                buffer.put(value);
            }
        }
    }

    private static Map<String, byte[]> readHeaders(ByteBuffer buffer) {
        int count = buffer.getShort() & 0xFFFF;
        Map<String, byte[]> headers = new HashMap<>();
        for (int i = 0; i < count; i++) {
            int keyLen = buffer.getShort() & 0xFFFF;
            byte[] keyBytes = new byte[keyLen];
            buffer.get(keyBytes);
            int valueLen = buffer.getShort() & 0xFFFF;
            byte[] valueBytes = valueLen == 0 ? null : new byte[valueLen];
            if (valueLen > 0) {
                buffer.get(valueBytes);
            }
            headers.put(new String(keyBytes, StandardCharsets.UTF_8), valueBytes);
        }
        return headers;
    }
}
