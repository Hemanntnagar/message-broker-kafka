package com.javastream.broker;

import com.javastream.exception.ProtocolDecodeException;
import com.javastream.partition.PartitionId;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.ProtocolConstants;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.Request;
import com.javastream.protocol.Response;
import com.javastream.protocol.StatusCode;
import com.javastream.storage.BrokerMessageStore;
import com.javastream.storage.BrokerMessageStore.StoredRecord;
import com.javastream.topic.TopicMetadata;
import com.javastream.topic.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Handles produce/fetch/topic operations for connected TCP clients. */
public final class BrokerRequestDispatcher {

    private static final Logger log = LoggerFactory.getLogger(BrokerRequestDispatcher.class);

    private final BrokerServices services;

    public BrokerRequestDispatcher(BrokerServices services) {
        this.services = services;
    }

    public Response dispatch(Request request) {
        try {
            return switch (request.operation()) {
                case PING -> Response.ok(request.correlationId(), new byte[0]);
                case CREATE_TOPIC -> handleCreateTopic(request);
                case PRODUCE -> handleProduce(request);
                case FETCH -> handleFetch(request);
                case CONSUMER_ACK -> handleAck(request);
                case JOIN_GROUP -> handleJoinGroup(request);
                case HEARTBEAT -> handleHeartbeat(request);
                case LEAVE_GROUP -> handleLeaveGroup(request);
                case COMMIT_OFFSETS -> handleCommitOffsets(request);
                case FETCH_COMMITTED_OFFSETS -> handleFetchCommittedOffsets(request);
                case LIST_TOPICS -> handleListTopics(request);
                case GET_METADATA -> handleGetMetadata(request);
                case DELETE_TOPIC -> handleDeleteTopic(request);
                default ->
                        new Response(
                                ProtocolConstants.VERSION_1,
                                request.correlationId(),
                                StatusCode.UNKNOWN_OPERATION,
                                new byte[0]);
            };
        } catch (ProtocolDecodeException e) {
            log.debug("Malformed request payload: {}", e.getMessage());
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.MALFORMED, new byte[0]);
        } catch (com.javastream.exception.JavaStreamException e) {
            log.debug("Bad request: {}", e.getMessage());
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        } catch (IllegalArgumentException | IllegalStateException e) {
            log.debug("Bad request: {}", e.getMessage());
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        }
    }

    private Response handleCreateTopic(Request request) {
        RecordPayloadCodec.CreateTopicRequest create = RecordPayloadCodec.decodeCreateTopicRequest(request.payload());
        String validated = com.javastream.topic.TopicNameValidator.validate(create.topic());
        TopicName topic = new TopicName(validated);
        if (services.partitionCount(topic).isPresent()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        }
        if (create.partitionCount() <= 0) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        }
        services.registerTopic(topic, create.partitionCount());
        return Response.ok(request.correlationId(), new byte[0]);
    }

    private Response handleListTopics(Request request) {
        List<RecordPayloadCodec.TopicEntry> entries = new ArrayList<>();
        for (String name : services.listTopics()) {
            TopicName topic = new TopicName(name);
            int count = services.partitionCount(topic).orElse(0);
            entries.add(new RecordPayloadCodec.TopicEntry(name, count));
        }
        RecordPayloadCodec.ListTopicsResponse list = new RecordPayloadCodec.ListTopicsResponse(entries);
        return Response.ok(request.correlationId(), RecordPayloadCodec.encodeListTopicsResponse(list));
    }

    private Response handleGetMetadata(Request request) {
        RecordPayloadCodec.GetMetadataRequest meta = RecordPayloadCodec.decodeGetMetadataRequest(request.payload());
        TopicName topic = new TopicName(meta.topic());
        Optional<TopicMetadata> metadata = services.topicMetadata(topic);
        if (metadata.isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        return Response.ok(
                request.correlationId(), RecordPayloadCodec.encodeGetMetadataResponse(metadata.get()));
    }

    private Response handleDeleteTopic(Request request) {
        RecordPayloadCodec.DeleteTopicRequest delete = RecordPayloadCodec.decodeDeleteTopicRequest(request.payload());
        TopicName topic = new TopicName(delete.topic());
        if (services.partitionCount(topic).isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        services.deleteTopic(topic);
        return Response.ok(request.correlationId(), new byte[0]);
    }

    private Response handleProduce(Request request) {
        RecordPayloadCodec.ProduceRequest produce = RecordPayloadCodec.decodeProduceRequest(request.payload());
        TopicName topic = new TopicName(produce.topic());
        if (services.partitionCount(topic).isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        int partition = services.resolvePartition(topic, produce.partitionHint(), produce.key());
        PartitionId partitionId = new PartitionId(topic, partition);
        BrokerMessageStore.AppendResult append =
                services.appendRecord(
                        partitionId,
                        produce.key(),
                        produce.value(),
                        produce.headers(),
                        System.currentTimeMillis());
        RecordPayloadCodec.ProduceResponse ack =
                new RecordPayloadCodec.ProduceResponse(append.messageId(), partition, append.offset());
        return Response.ok(request.correlationId(), RecordPayloadCodec.encodeProduceResponse(ack));
    }

    private Response handleFetch(Request request) {
        RecordPayloadCodec.FetchRequest fetch = RecordPayloadCodec.decodeFetchRequest(request.payload());
        TopicName topic = new TopicName(fetch.topic());
        if (services.partitionCount(topic).isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        int partition = fetch.partition();
        Integer partitions = services.partitionCount(topic).orElse(0);
        if (partition < 0 || partition >= partitions) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        }
        PartitionId partitionId = new PartitionId(topic, partition);
        List<StoredRecord> stored =
                services.messageStore().fetch(partitionId, fetch.offset(), Math.max(1, fetch.maxRecords()));
        List<RecordPayloadCodec.FetchedRecord> records = new ArrayList<>(stored.size());
        for (StoredRecord record : stored) {
            records.add(
                    new RecordPayloadCodec.FetchedRecord(
                            record.messageId(),
                            partition,
                            record.offset(),
                            record.timestampEpochMillis(),
                            record.key(),
                            record.value(),
                            record.headers()));
        }
        RecordPayloadCodec.FetchResponse response = new RecordPayloadCodec.FetchResponse(records);
        return Response.ok(request.correlationId(), RecordPayloadCodec.encodeFetchResponse(response));
    }

    private Response handleAck(Request request) {
        RecordPayloadCodec.AckRequest ack = RecordPayloadCodec.decodeAckRequest(request.payload());
        TopicName topic = new TopicName(ack.topic());
        if (services.partitionCount(topic).isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        services.acknowledge(topic, ack.partition(), ack.offset());
        return Response.ok(request.correlationId(), new byte[0]);
    }

    private Response handleJoinGroup(Request request) {
        RecordPayloadCodec.JoinGroupRequest join = RecordPayloadCodec.decodeJoinGroupRequest(request.payload());
        var result =
                services.consumerGroupManager()
                        .joinGroup(join.groupId(), join.memberId(), join.topic());
        List<RecordPayloadCodec.CommittedOffsetEntry> committed = new ArrayList<>();
        for (var entry : result.committedOffsets().entrySet()) {
            committed.add(new RecordPayloadCodec.CommittedOffsetEntry(entry.getKey(), entry.getValue()));
        }
        RecordPayloadCodec.JoinGroupResponse response =
                new RecordPayloadCodec.JoinGroupResponse(
                        result.generation(),
                        result.assignedPartitions(),
                        result.revokedPartitions(),
                        committed);
        return Response.ok(request.correlationId(), RecordPayloadCodec.encodeJoinGroupResponse(response));
    }

    private Response handleHeartbeat(Request request) {
        RecordPayloadCodec.HeartbeatRequest heartbeat = RecordPayloadCodec.decodeHeartbeatRequest(request.payload());
        services.consumerGroupManager().heartbeat(heartbeat.groupId(), heartbeat.memberId());
        return Response.ok(request.correlationId(), new byte[0]);
    }

    private Response handleLeaveGroup(Request request) {
        RecordPayloadCodec.LeaveGroupRequest leave = RecordPayloadCodec.decodeLeaveGroupRequest(request.payload());
        services.consumerGroupManager().leaveGroup(leave.groupId(), leave.memberId());
        return Response.ok(request.correlationId(), new byte[0]);
    }

    private Response handleCommitOffsets(Request request) {
        RecordPayloadCodec.CommitOffsetsRequest commit =
                RecordPayloadCodec.decodeCommitOffsetsRequest(request.payload());
        TopicName topic = new TopicName(commit.topic());
        if (services.partitionCount(topic).isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        java.util.Map<Integer, Long> offsets = new java.util.LinkedHashMap<>();
        for (RecordPayloadCodec.CommittedOffsetEntry entry : commit.offsets()) {
            offsets.put(entry.partition(), entry.offset());
        }
        services.consumerGroupManager().commitOffsets(commit.groupId(), commit.topic(), offsets);
        return Response.ok(request.correlationId(), new byte[0]);
    }

    private Response handleFetchCommittedOffsets(Request request) {
        RecordPayloadCodec.FetchCommittedOffsetsRequest fetch =
                RecordPayloadCodec.decodeFetchCommittedOffsetsRequest(request.payload());
        TopicName topic = new TopicName(fetch.topic());
        if (services.partitionCount(topic).isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        var offsets =
                services.consumerGroupManager()
                        .fetchCommittedOffsets(fetch.groupId(), fetch.topic(), fetch.partitions());
        List<RecordPayloadCodec.CommittedOffsetEntry> entries = new ArrayList<>();
        for (var entry : offsets.entrySet()) {
            entries.add(new RecordPayloadCodec.CommittedOffsetEntry(entry.getKey(), entry.getValue()));
        }
        return Response.ok(
                request.correlationId(),
                RecordPayloadCodec.encodeFetchCommittedOffsetsResponse(
                        new RecordPayloadCodec.FetchCommittedOffsetsResponse(entries)));
    }
}
