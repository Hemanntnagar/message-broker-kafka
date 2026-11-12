package com.javastream.producer;

import com.javastream.exception.ClientNotConnectedException;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.RecordPayloadCodec.ProduceRequest;
import com.javastream.client.TcpProtocolConnection;

import java.io.IOException;
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
        RecordPayloadCodec.ProduceResponse ack = RecordPayloadCodec.decodeProduceResponse(response.payload());
        return new PublishAcknowledgement(ack.messageId(), topic, ack.partition(), ack.offset());
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
