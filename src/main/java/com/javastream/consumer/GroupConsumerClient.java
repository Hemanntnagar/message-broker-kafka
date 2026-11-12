package com.javastream.consumer;

import com.javastream.client.TcpProtocolConnection;
import com.javastream.exception.ClientNotConnectedException;
import com.javastream.message.Message;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.RecordPayloadCodec.CommittedOffsetEntry;
import com.javastream.protocol.RecordPayloadCodec.FetchedRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TCP consumer that participates in a consumer group: join, heartbeat, partition assignment, and offset commits.
 */
public final class GroupConsumerClient implements AutoCloseable {

    private final TcpProtocolConnection connection;
    private final Map<Integer, Long> nextOffsets = new ConcurrentHashMap<>();
    private final AtomicBoolean heartbeatRunning = new AtomicBoolean(false);
    private Thread heartbeatThread;

    private volatile String groupId;
    private volatile String memberId;
    private volatile String topic;
    private volatile List<Integer> assignedPartitions = List.of();
    private volatile int batchSize = 100;

    public GroupConsumerClient(String host, int port) {
        this(new TcpProtocolConnection(host, port));
    }

    GroupConsumerClient(TcpProtocolConnection connection) {
        this.connection = Objects.requireNonNull(connection, "connection");
    }

    public void connect() throws IOException {
        connection.connect();
    }

    public void disconnect() {
        stopHeartbeat();
        connection.close();
    }

    public boolean isConnected() {
        return connection.isConnected();
    }

    public void joinGroup(String groupId, String memberId, String topic) throws IOException {
        ensureConnected();
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.memberId = Objects.requireNonNull(memberId, "memberId");
        this.topic = Objects.requireNonNull(topic, "topic");

        RecordPayloadCodec.JoinGroupRequest request = new RecordPayloadCodec.JoinGroupRequest(groupId, memberId, topic);
        var response = connection.send(OperationCode.JOIN_GROUP, RecordPayloadCodec.encodeJoinGroupRequest(request));
        RecordPayloadCodec.JoinGroupResponse join = RecordPayloadCodec.decodeJoinGroupResponse(response.payload());

        for (Integer revoked : join.revokedPartitions()) {
            nextOffsets.remove(revoked);
        }
        assignedPartitions = List.copyOf(join.assignedPartitions());
        for (Integer partition : assignedPartitions) {
            long committed =
                    join.committedOffsets().stream()
                            .filter(entry -> entry.partition() == partition)
                            .map(CommittedOffsetEntry::offset)
                            .findFirst()
                            .orElse(-1L);
            long resume = committed < 0 ? 0L : committed + 1;
            nextOffsets.put(partition, resume);
        }
        startHeartbeat();
    }

    public List<Integer> assignedPartitions() {
        return assignedPartitions;
    }

    public void setBatchSize(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.batchSize = batchSize;
    }

    public List<Message> poll(Duration timeout) throws IOException {
        ensureConnected();
        ensureJoined();
        long deadline = System.nanoTime() + timeout.toNanos();
        List<Message> messages = new ArrayList<>();
        for (int partition : assignedPartitions) {
            if (System.nanoTime() >= deadline) {
                break;
            }
            long offset = nextOffsets.getOrDefault(partition, 0L);
            int remainingMillis = (int) Math.max(1, (deadline - System.nanoTime()) / 1_000_000);
            RecordPayloadCodec.FetchRequest request =
                    new RecordPayloadCodec.FetchRequest(topic, partition, offset, batchSize, remainingMillis);
            var response = connection.send(OperationCode.FETCH, RecordPayloadCodec.encodeFetchRequest(request));
            RecordPayloadCodec.FetchResponse fetch = RecordPayloadCodec.decodeFetchResponse(response.payload());
            for (FetchedRecord record : fetch.records()) {
                messages.add(
                        new Message(
                                record.messageId(),
                                record.key(),
                                record.value(),
                                record.headers(),
                                record.timestampEpochMillis(),
                                topic,
                                record.partition(),
                                record.offset()));
                nextOffsets.put(partition, record.offset() + 1);
            }
        }
        return List.copyOf(messages);
    }

    public void commitOffsets(Map<Integer, Long> offsets) throws IOException {
        ensureConnected();
        ensureJoined();
        List<CommittedOffsetEntry> entries = new ArrayList<>();
        for (var entry : offsets.entrySet()) {
            entries.add(new CommittedOffsetEntry(entry.getKey(), entry.getValue()));
        }
        RecordPayloadCodec.CommitOffsetsRequest request =
                new RecordPayloadCodec.CommitOffsetsRequest(groupId, topic, entries);
        connection.send(OperationCode.COMMIT_OFFSETS, RecordPayloadCodec.encodeCommitOffsetsRequest(request));
    }

    public Map<Integer, Long> fetchCommittedOffsets(List<Integer> partitions) throws IOException {
        ensureConnected();
        ensureJoined();
        RecordPayloadCodec.FetchCommittedOffsetsRequest request =
                new RecordPayloadCodec.FetchCommittedOffsetsRequest(groupId, topic, partitions);
        var response =
                connection.send(
                        OperationCode.FETCH_COMMITTED_OFFSETS,
                        RecordPayloadCodec.encodeFetchCommittedOffsetsRequest(request));
        RecordPayloadCodec.FetchCommittedOffsetsResponse committed =
                RecordPayloadCodec.decodeFetchCommittedOffsetsResponse(response.payload());
        Map<Integer, Long> offsets = new LinkedHashMap<>();
        for (CommittedOffsetEntry entry : committed.offsets()) {
            offsets.put(entry.partition(), entry.offset());
        }
        return Map.copyOf(offsets);
    }

    public void leaveGroup() throws IOException {
        if (groupId == null || memberId == null) {
            return;
        }
        ensureConnected();
        stopHeartbeat();
        RecordPayloadCodec.LeaveGroupRequest request = new RecordPayloadCodec.LeaveGroupRequest(groupId, memberId);
        connection.send(OperationCode.LEAVE_GROUP, RecordPayloadCodec.encodeLeaveGroupRequest(request));
        assignedPartitions = List.of();
        nextOffsets.clear();
        groupId = null;
        memberId = null;
        topic = null;
    }

  private void startHeartbeat() {
        if (!heartbeatRunning.compareAndSet(false, true)) {
            return;
        }
        heartbeatThread =
                Thread.ofVirtual()
                        .name("group-consumer-heartbeat")
                        .start(
                                () -> {
                                    while (heartbeatRunning.get() && connection.isConnected()) {
                                        try {
                                            RecordPayloadCodec.HeartbeatRequest heartbeat =
                                                    new RecordPayloadCodec.HeartbeatRequest(groupId, memberId);
                                            connection.send(
                                                    OperationCode.HEARTBEAT,
                                                    RecordPayloadCodec.encodeHeartbeatRequest(heartbeat));
                                            Thread.sleep(2_000);
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            break;
                                        } catch (IOException ignored) {
                                            break;
                                        }
                                    }
                                });
    }

    private void stopHeartbeat() {
        heartbeatRunning.set(false);
        Thread thread = heartbeatThread;
        if (thread != null) {
            thread.interrupt();
        }
    }

    private void ensureConnected() {
        if (!connection.isConnected()) {
            throw new ClientNotConnectedException("Group consumer is not connected");
        }
    }

    private void ensureJoined() {
        if (groupId == null || topic == null) {
            throw new IllegalStateException("joinGroup must be called before polling or committing offsets");
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (connection.isConnected()) {
                leaveGroup();
            }
        } catch (Exception ignored) {
            // best effort while closing
        }
        disconnect();
    }
}
