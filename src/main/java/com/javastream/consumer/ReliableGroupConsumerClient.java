package com.javastream.consumer;

import com.javastream.delivery.ConsumerDeliverySettings;
import com.javastream.message.Message;
import com.javastream.producer.ProducerClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Group consumer that commits offsets after successful processing, retries failures, and can route poison pills to a
 * dead-letter topic (at-least-once).
 */
public final class ReliableGroupConsumerClient implements AutoCloseable {

    private final GroupConsumerClient delegate;
    private final ConsumerDeliverySettings settings;
    private final String brokerHost;
    private final int brokerPort;

    public ReliableGroupConsumerClient(
            String host, int port, GroupConsumerClient delegate, ConsumerDeliverySettings settings) {
        this.brokerHost = Objects.requireNonNull(host, "host");
        this.brokerPort = port;
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.settings = Objects.requireNonNull(settings, "settings");
    }

    public ReliableGroupConsumerClient(String host, int port, ConsumerDeliverySettings settings) {
        this(host, port, new GroupConsumerClient(host, port), settings);
    }

    public void connect() throws IOException {
        delegate.connect();
    }

    public void joinGroup(String groupId, String memberId, String topic) throws IOException {
        delegate.joinGroup(groupId, memberId, topic);
    }

    public int processOnce(MessageHandler handler, Duration pollTimeout) throws IOException {
        List<Message> batch = delegate.poll(pollTimeout);
        int processed = 0;
        for (Message message : batch) {
            processMessage(handler, message);
            processed++;
        }
        return processed;
    }

    public void runUntilInterrupted(MessageHandler handler, Duration pollTimeout) throws IOException {
        while (!Thread.currentThread().isInterrupted()) {
            processOnce(handler, pollTimeout);
        }
    }

    private void processMessage(MessageHandler handler, Message message) throws IOException {
        int attempts = 0;
        while (true) {
            try {
                handler.handle(message);
                if (settings.commitAfterSuccessfulProcessing()) {
                    commitAfterProcessing(message);
                }
                return;
            } catch (RetryableProcessingException e) {
                attempts++;
                if (attempts > settings.maxProcessingRetries()) {
                    sendToDeadLetter(message, e);
                    if (settings.commitAfterSuccessfulProcessing()) {
                        commitAfterProcessing(message);
                    }
                    return;
                }
            }
        }
    }

    private void commitAfterProcessing(Message message) throws IOException {
        if (settings.autoCommitOffsets()) {
            delegate.commitOffsets(Map.of(message.partition(), message.offset()));
        }
    }

    /** Manual offset commit for at-least-once pipelines that commit in batches. */
    public void commitOffsets(Map<Integer, Long> offsets) throws IOException {
        delegate.commitOffsets(offsets);
    }

    public Map<Integer, Long> fetchCommittedOffsets(List<Integer> partitions) throws IOException {
        return delegate.fetchCommittedOffsets(partitions);
    }

    private void sendToDeadLetter(Message message, Exception failure) throws IOException {
        String dlqTopic = settings.deadLetterTopic();
        if (dlqTopic == null) {
            return;
        }
        try (ProducerClient dlqProducer = new ProducerClient(brokerHost, brokerPort)) {
            dlqProducer.connect();
            Map<String, byte[]> headers = new LinkedHashMap<>(message.headers());
            headers.put("dlq.source.topic", message.topic().getBytes(StandardCharsets.UTF_8));
            headers.put("dlq.source.partition", Integer.toString(message.partition()).getBytes(StandardCharsets.UTF_8));
            headers.put("dlq.source.offset", Long.toString(message.offset()).getBytes(StandardCharsets.UTF_8));
            headers.put(
                    "dlq.error", failure.getMessage() == null ? new byte[0] : failure.getMessage().getBytes(StandardCharsets.UTF_8));
            dlqProducer.publish(dlqTopic, message.key(), message.value(), headers);
        }
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
