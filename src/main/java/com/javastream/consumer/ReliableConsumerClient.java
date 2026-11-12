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
 * Standalone consumer that acknowledges (or commits via {@link ConsumerClient#acknowledge}) only after successful
 * processing.
 */
public final class ReliableConsumerClient implements AutoCloseable {

    private final ConsumerClient delegate;
    private final ConsumerDeliverySettings settings;
    private final String brokerHost;
    private final int brokerPort;

    public ReliableConsumerClient(String host, int port, ConsumerDeliverySettings settings) {
        this(new ConsumerClient(host, port), host, port, settings);
    }

    ReliableConsumerClient(ConsumerClient delegate, String host, int port, ConsumerDeliverySettings settings) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.brokerHost = host;
        this.brokerPort = port;
        this.settings = Objects.requireNonNull(settings, "settings");
    }

    public void connect() throws IOException {
        delegate.connect();
    }

    public void subscribe(String topic) {
        delegate.subscribe(topic);
    }

    public void setPartition(int partition) {
        delegate.setPartition(partition);
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

    private void processMessage(MessageHandler handler, Message message) throws IOException {
        int attempts = 0;
        while (true) {
            try {
                handler.handle(message);
                if (settings.commitAfterSuccessfulProcessing()) {
                    delegate.acknowledge(message);
                }
                return;
            } catch (RetryableProcessingException e) {
                attempts++;
                if (attempts > settings.maxProcessingRetries()) {
                    sendToDeadLetter(message, e);
                    if (settings.commitAfterSuccessfulProcessing()) {
                        delegate.acknowledge(message);
                    }
                    return;
                }
            }
        }
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
    public void close() {
        delegate.close();
    }
}
