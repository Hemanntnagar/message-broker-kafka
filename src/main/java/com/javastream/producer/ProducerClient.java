package com.javastream.producer;

import com.javastream.exception.ClientNotConnectedException;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.RecordPayloadCodec.ProduceRequest;
import com.javastream.client.TcpProtocolConnection;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * TCP producer client with synchronous {@link #publish} calls.
 */
public final class ProducerClient implements AutoCloseable {

    private final TcpProtocolConnection connection;

    public ProducerClient(String host, int port) {
        this(new TcpProtocolConnection(host, port));
    }

    ProducerClient(TcpProtocolConnection connection) {
        this.connection = Objects.requireNonNull(connection, "connection");
    }

    public void connect() throws IOException {
        connection.connect();
    }

    public void disconnect() {
        connection.close();
    }

    public boolean isConnected() {
        return connection.isConnected();
    }

    public PublishAcknowledgement publish(
            String topic, byte[] key, byte[] value, Map<String, byte[]> headers) throws IOException {
        return publish(topic, RecordPayloadCodec.AUTO_PARTITION, key, value, headers);
    }

    public PublishAcknowledgement publish(
            String topic, int partitionHint, byte[] key, byte[] value, Map<String, byte[]> headers)
            throws IOException {
        ensureConnected();
        ProduceRequest request = new ProduceRequest(topic, partitionHint, key, value, headers);
        var response = connection.send(OperationCode.PRODUCE, RecordPayloadCodec.encodeProduceRequest(request));
        RecordPayloadCodec.ProduceResponse ack =
                RecordPayloadCodec.decodeProduceResponse(response.payload()).responses().getFirst();
        return new PublishAcknowledgement(ack.messageId(), topic, ack.partition(), ack.offset());
    }

    /**
     * Publishes multiple records in one round trip when {@code records.size() > 1}; otherwise delegates to
     * {@link #publish(String, int, byte[], byte[], Map)}.
     */
    public List<PublishAcknowledgement> publishBatch(
            String topic, int partitionHint, List<RecordPayloadCodec.ProduceRecord> records) throws IOException {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("records must not be empty");
        }
        if (records.size() == 1) {
            RecordPayloadCodec.ProduceRecord only = records.getFirst();
            return List.of(
                    publish(topic, partitionHint, only.key(), only.value(), only.headers()));
        }
        ensureConnected();
        RecordPayloadCodec.ProduceRecord head = records.getFirst();
        List<RecordPayloadCodec.ProduceRecord> tail = List.copyOf(records.subList(1, records.size()));
        ProduceRequest request =
                new ProduceRequest(
                        topic,
                        partitionHint,
                        head.key(),
                        head.value(),
                        head.headers(),
                        com.javastream.delivery.ProducerAckLevel.ACK_LEADER,
                        0,
                        -1,
                        0,
                        tail);
        var response = connection.send(OperationCode.PRODUCE, RecordPayloadCodec.encodeProduceRequest(request));
        List<PublishAcknowledgement> acks = new java.util.ArrayList<>();
        for (RecordPayloadCodec.ProduceResponse item :
                RecordPayloadCodec.decodeProduceResponse(response.payload()).responses()) {
            acks.add(new PublishAcknowledgement(item.messageId(), topic, item.partition(), item.offset()));
        }
        return List.copyOf(acks);
    }

    public void createTopic(String topic, int partitionCount) throws IOException {
        ensureConnected();
        RecordPayloadCodec.CreateTopicRequest request =
                new RecordPayloadCodec.CreateTopicRequest(topic, partitionCount);
        connection.send(OperationCode.CREATE_TOPIC, RecordPayloadCodec.encodeCreateTopicRequest(request));
    }

    private void ensureConnected() {
        if (!connection.isConnected()) {
            throw new ClientNotConnectedException("Producer is not connected");
        }
    }

    @Override
    public void close() {
        disconnect();
    }
}
