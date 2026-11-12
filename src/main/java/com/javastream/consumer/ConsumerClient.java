package com.javastream.consumer;

import com.javastream.exception.ClientNotConnectedException;
import com.javastream.exception.JavaStreamException;
import com.javastream.message.Message;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.RecordPayloadCodec.FetchedRecord;
import com.javastream.client.TcpProtocolConnection;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TCP consumer client supporting subscription, batch polling, and synchronous acknowledgement.
 */
public final class ConsumerClient implements AutoCloseable {

    private final TcpProtocolConnection connection;
    private final ConcurrentHashMap<String, Long> nextOffsets = new ConcurrentHashMap<>();
    private volatile String subscribedTopic;
    private volatile int batchSize = 100;
    private volatile int partition = 0;

    public ConsumerClient(String host, int port) {
        this(new TcpProtocolConnection(host, port));
    }

    ConsumerClient(TcpProtocolConnection connection) {
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

    public void subscribe(String topic) {
        subscribedTopic = Objects.requireNonNull(topic, "topic");
        nextOffsets.putIfAbsent(topic, 0L);
    }

    public String subscribedTopic() {
        return subscribedTopic;
    }

    public void setBatchSize(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.batchSize = batchSize;
    }

    public void setPartition(int partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative");
        }
        this.partition = partition;
    }

    public List<Message> poll(Duration timeout) throws IOException {
        ensureConnected();
        String topic = subscribedTopic;
        if (topic == null) {
            throw new IllegalStateException("Subscribe to a topic before polling");
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        List<Message> messages = new ArrayList<>();
        while (System.nanoTime() < deadline) {
            long offset = nextOffsets.getOrDefault(topic, 0L);
            RecordPayloadCodec.FetchRequest request =
                    new RecordPayloadCodec.FetchRequest(topic, partition, offset, batchSize, (int) timeout.toMillis());
            var response = connection.send(OperationCode.FETCH, RecordPayloadCodec.encodeFetchRequest(request));
            RecordPayloadCodec.FetchResponse fetch = RecordPayloadCodec.decodeFetchResponse(response.payload());
            if (fetch.records().isEmpty()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                try {
                    Thread.sleep(Math.min(50, Math.max(1, remaining / 1_000_000)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                continue;
            }
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
                nextOffsets.put(topic, record.offset() + 1);
            }
            break;
        }
        return List.copyOf(messages);
    }

    public void acknowledge(Message message) throws IOException {
        ensureConnected();
        RecordPayloadCodec.AckRequest ack =
                new RecordPayloadCodec.AckRequest(message.topic(), message.partition(), message.offset());
        connection.send(OperationCode.CONSUMER_ACK, RecordPayloadCodec.encodeAckRequest(ack));
    }

    private void ensureConnected() {
        if (!connection.isConnected()) {
            throw new ClientNotConnectedException("Consumer is not connected");
        }
    }

    @Override
    public void close() {
        disconnect();
    }
}
