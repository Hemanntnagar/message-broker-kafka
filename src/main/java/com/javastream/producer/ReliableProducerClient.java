package com.javastream.producer;

import com.javastream.client.TcpProtocolConnection;
import com.javastream.delivery.ProducerDeliverySettings;
import com.javastream.exception.ClientNotConnectedException;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.RecordPayloadCodec.ProduceRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producer with configurable ack levels, per-request timeouts, retries, and optional idempotent sequence numbers.
 */
public final class ReliableProducerClient implements AutoCloseable {

    private final TcpProtocolConnection connection;
    private final ProducerDeliverySettings settings;
    private final AtomicLong nextProducerId = new AtomicLong(-1);
    private final ConcurrentHashMap<String, TopicSequence> sequencesByTopic = new ConcurrentHashMap<>();

    public ReliableProducerClient(String host, int port) {
        this(host, port, ProducerDeliverySettings.DEFAULTS);
    }

    public ReliableProducerClient(String host, int port, ProducerDeliverySettings settings) {
        this(new TcpProtocolConnection(host, port), settings);
    }

    public ReliableProducerClient(TcpProtocolConnection connection, ProducerDeliverySettings settings) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.settings = Objects.requireNonNull(settings, "settings");
        if (settings.idempotent()) {
            nextProducerId.compareAndSet(-1, settings.producerId());
        }
    }

    public void connect() throws IOException {
        connection.connect();
    }

    public void disconnect() {
        connection.close();
    }

    public long producerId() {
        if (!settings.idempotent()) {
            return -1;
        }
        long id = nextProducerId.get();
        if (id < 0) {
            id = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
            nextProducerId.compareAndSet(-1, id);
        }
        return id;
    }

    public ProducerDeliverySettings settings() {
        return settings;
    }

    public void createTopic(String topic, int partitionCount) throws IOException {
        ensureConnected();
        RecordPayloadCodec.CreateTopicRequest request =
                new RecordPayloadCodec.CreateTopicRequest(topic, partitionCount);
        connection.send(OperationCode.CREATE_TOPIC, RecordPayloadCodec.encodeCreateTopicRequest(request));
    }

    public PublishAcknowledgement publish(
            String topic, byte[] key, byte[] value, Map<String, byte[]> headers) throws IOException {
        return publish(topic, RecordPayloadCodec.AUTO_PARTITION, key, value, headers);
    }

    public PublishAcknowledgement publish(
            String topic, int partitionHint, byte[] key, byte[] value, Map<String, byte[]> headers)
            throws IOException {
        ensureConnected();
        long producerId = settings.idempotent() ? producerId() : -1;
        TopicSequence topicSequence =
                producerId >= 0
                        ? sequencesByTopic.computeIfAbsent(topic, ignored -> new TopicSequence())
                        : null;
        long sequence = topicSequence != null ? topicSequence.nextToSend() : 0;
        IOException lastFailure = null;
        for (int attempt = 0; attempt <= settings.maxRetries(); attempt++) {
            try {
                if (attempt > 0) {
                    Thread.sleep(settings.retryBackoff().toMillis());
                }
                ProduceRequest request =
                        new ProduceRequest(
                                topic,
                                partitionHint,
                                key,
                                value,
                                headers,
                                settings.ackLevel(),
                                Math.toIntExact(settings.requestTimeout().toMillis()),
                                producerId,
                                sequence,
                                List.of());
                var response =
                        connection.send(
                                OperationCode.PRODUCE,
                                RecordPayloadCodec.encodeProduceRequest(request),
                                settings.requestTimeout());
                RecordPayloadCodec.ProduceResponse ack =
                        RecordPayloadCodec.decodeProduceResponse(response.payload()).responses().getFirst();
                if (topicSequence != null) {
                    topicSequence.markCommitted(sequence);
                }
                return new PublishAcknowledgement(ack.messageId(), topic, ack.partition(), ack.offset());
            } catch (IOException e) {
                lastFailure = e;
                if (!connection.isConnected()) {
                    connection.connect();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while retrying produce", e);
            }
        }
        throw lastFailure != null ? lastFailure : new IOException("Produce failed after retries");
    }

    private void ensureConnected() {
        if (!connection.isConnected()) {
            throw new ClientNotConnectedException("Producer is not connected");
        }
    }

    @Override
    public void close() {
        connection.close();
    }

    private static final class TopicSequence {
        private long nextToSend;

        synchronized long nextToSend() {
            return nextToSend;
        }

        synchronized void markCommitted(long sequence) {
            if (sequence == nextToSend) {
                nextToSend++;
            }
        }
    }
}
