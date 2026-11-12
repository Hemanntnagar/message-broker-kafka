package com.javastream.broker;

import com.javastream.exception.ProtocolDecodeException;
import com.javastream.partition.PartitionId;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.ProtocolConstants;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.Request;
import com.javastream.protocol.Response;
import com.javastream.protocol.StatusCode;
import com.javastream.storage.InMemoryMessageStore;
import com.javastream.storage.InMemoryMessageStore.StoredRecord;
import com.javastream.topic.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
                case LIST_TOPICS, GET_METADATA ->
                        Response.ok(request.correlationId(), new byte[0]);
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
        } catch (IllegalArgumentException | IllegalStateException e) {
            log.debug("Bad request: {}", e.getMessage());
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        }
    }

    private Response handleCreateTopic(Request request) {
        RecordPayloadCodec.CreateTopicRequest create = RecordPayloadCodec.decodeCreateTopicRequest(request.payload());
        TopicName topic = new TopicName(create.topic());
        if (services.partitionCount(topic).isPresent()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        }
        services.registerTopic(topic, create.partitionCount());
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
        InMemoryMessageStore.AppendResult append =
                services.messageStore()
                        .appendRecord(
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
}
