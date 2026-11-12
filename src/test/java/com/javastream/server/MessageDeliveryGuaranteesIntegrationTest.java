package com.javastream.server;

import com.javastream.broker.BrokerRequestDispatcher;
import com.javastream.broker.BrokerServices;
import com.javastream.client.FailureInjectingConnection;
import com.javastream.client.TcpProtocolConnection;
import com.javastream.consumer.ConsumerClient;
import com.javastream.consumer.GroupConsumerClient;
import com.javastream.consumer.ReliableConsumerClient;
import com.javastream.consumer.ReliableGroupConsumerClient;
import com.javastream.consumer.RetryableProcessingException;
import com.javastream.delivery.ConsumerDeliverySettings;
import com.javastream.delivery.ProducerAckLevel;
import com.javastream.delivery.ProducerDeliverySettings;
import com.javastream.message.Message;
import com.javastream.producer.ProducerClient;
import com.javastream.producer.PublishAcknowledgement;
import com.javastream.producer.ReliableProducerClient;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.RecordPayloadCodec.ProduceRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageDeliveryGuaranteesIntegrationTest {

    private static final int MAX_FRAME = 65_536;
    private BrokerServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void producerRetriesAfterInjectedNetworkFailure() throws Exception {
        server = startBroker();
        TcpProtocolConnection base = new TcpProtocolConnection("127.0.0.1", server.boundPort());
        FailureInjectingConnection flaky = new FailureInjectingConnection(base, 1);
        ProducerDeliverySettings settings =
                ProducerDeliverySettings.builder()
                        .maxRetries(2)
                        .retryBackoff(Duration.ofMillis(10))
                        .requestTimeout(Duration.ofSeconds(5))
                        .build();
        try (ReliableProducerClient producer = new ReliableProducerClient(flaky, settings)) {
            producer.connect();
            producer.createTopic("retry-topic", 1);
            PublishAcknowledgement ack =
                    producer.publish("retry-topic", null, "payload".getBytes(StandardCharsets.UTF_8), Map.of());
            assertEquals(0, ack.offset());
        }
    }

    @Test
    void idempotentProducerDeduplicatesRetries() throws Exception {
        server = startBroker();
        var settings =
                ProducerDeliverySettings.builder()
                        .idempotent(42L)
                        .maxRetries(3)
                        .retryBackoff(Duration.ofMillis(5))
                        .requestTimeout(Duration.ofSeconds(5))
                        .build();
        TcpProtocolConnection base = new TcpProtocolConnection("127.0.0.1", server.boundPort());
        FailureInjectingConnection flaky = new FailureInjectingConnection(base, 2);
        try (ReliableProducerClient producer = new ReliableProducerClient(flaky, settings)) {
            producer.connect();
            producer.createTopic("dedup-topic", 1);
            PublishAcknowledgement first =
                    producer.publish("dedup-topic", null, "one".getBytes(StandardCharsets.UTF_8), Map.of());
            PublishAcknowledgement second =
                    producer.publish("dedup-topic", null, "two".getBytes(StandardCharsets.UTF_8), Map.of());
            assertEquals(first.messageId() + 1, second.messageId());
        }
        try (ConsumerClient consumer = new ConsumerClient("127.0.0.1", server.boundPort())) {
            consumer.connect();
            consumer.subscribe("dedup-topic");
            assertEquals(2, consumer.poll(Duration.ofSeconds(2)).size());
        }
    }

    @Test
    void duplicateIdempotentWireRequestReturnsSameOffset() throws Exception {
        server = startBroker();
        try (ProducerClient setup = new ProducerClient("127.0.0.1", server.boundPort());
                TcpProtocolConnection connection = new TcpProtocolConnection("127.0.0.1", server.boundPort())) {
            setup.connect();
            setup.createTopic("wire-dedup", 1);
            connection.connect();

            ProduceRequest request =
                    new ProduceRequest(
                            "wire-dedup",
                            0,
                            null,
                            "x".getBytes(StandardCharsets.UTF_8),
                            Map.of(),
                            ProducerAckLevel.ACK_LEADER,
                            5_000,
                            99L,
                            0,
                            List.of());
            byte[] payload = RecordPayloadCodec.encodeProduceRequest(request);
            var first = connection.send(OperationCode.PRODUCE, payload);
            var second = connection.send(OperationCode.PRODUCE, payload);
            var ack1 = RecordPayloadCodec.decodeProduceResponse(first.payload()).responses().getFirst();
            var ack2 = RecordPayloadCodec.decodeProduceResponse(second.payload()).responses().getFirst();
            assertEquals(ack1.messageId(), ack2.messageId());
            assertEquals(ack1.offset(), ack2.offset());
        }
        try (ConsumerClient consumer = new ConsumerClient("127.0.0.1", server.boundPort())) {
            consumer.connect();
            consumer.subscribe("wire-dedup");
            assertEquals(1, consumer.poll(Duration.ofSeconds(2)).size());
        }
    }

    @Test
    void consumerCrashBeforeAckRedelivers() throws Exception {
        server = startBroker();
        try (ReliableProducerClient producer =
                new ReliableProducerClient("127.0.0.1", server.boundPort(), ProducerDeliverySettings.DEFAULTS)) {
            producer.connect();
            producer.createTopic("ack-topic", 1);
            producer.publish("ack-topic", null, "m".getBytes(StandardCharsets.UTF_8), Map.of());
        }
        try (ConsumerClient consumer = new ConsumerClient("127.0.0.1", server.boundPort())) {
            consumer.connect();
            consumer.subscribe("ack-topic");
            assertEquals(1, consumer.poll(Duration.ofSeconds(2)).size());
            // no acknowledge — simulate crash
        }
        try (ConsumerClient consumer = new ConsumerClient("127.0.0.1", server.boundPort())) {
            consumer.connect();
            consumer.subscribe("ack-topic");
            assertEquals(1, consumer.poll(Duration.ofSeconds(2)).size());
        }
    }

    @Test
    void reliableConsumerAcksAfterProcessing() throws Exception {
        server = startBroker();
        try (ProducerClient producer = new ProducerClient("127.0.0.1", server.boundPort())) {
            producer.connect();
            producer.createTopic("reliable", 1);
            producer.publish("reliable", null, "ok".getBytes(StandardCharsets.UTF_8), Map.of());
        }
        ConsumerDeliverySettings settings =
                new ConsumerDeliverySettings(true, 0, "reliable.dlq", true);
        try (ReliableConsumerClient consumer =
                new ReliableConsumerClient("127.0.0.1", server.boundPort(), settings)) {
            consumer.connect();
            consumer.subscribe("reliable");
            AtomicInteger handled = new AtomicInteger();
            consumer.processOnce(message -> handled.incrementAndGet(), Duration.ofSeconds(2));
            assertEquals(1, handled.get());
            assertEquals(0, consumer.processOnce(message -> handled.incrementAndGet(), Duration.ofMillis(300)));
        }
    }

    @Test
    void retryExhaustionSendsToDeadLetterTopic() throws Exception {
        server = startBroker();
        try (ProducerClient producer = new ProducerClient("127.0.0.1", server.boundPort())) {
            producer.connect();
            producer.createTopic("orders", 1);
            producer.createTopic("orders.dlq", 1);
            producer.publish("orders", null, "bad".getBytes(StandardCharsets.UTF_8), Map.of());
        }
        ConsumerDeliverySettings settings = ConsumerDeliverySettings.atLeastOnce("orders.dlq");
        try (ReliableGroupConsumerClient consumer =
                new ReliableGroupConsumerClient("127.0.0.1", server.boundPort(), settings)) {
            consumer.connect();
            consumer.joinGroup("retry-group", "m1", "orders");
            consumer.processOnce(
                    message -> {
                        throw new RetryableProcessingException("always fails");
                    },
                    Duration.ofSeconds(2));
        }
        try (ConsumerClient dlqReader = new ConsumerClient("127.0.0.1", server.boundPort())) {
            dlqReader.connect();
            dlqReader.subscribe("orders.dlq");
            List<Message> dead = dlqReader.poll(Duration.ofSeconds(2));
            assertEquals(1, dead.size());
            assertArrayEquals("bad", dead.getFirst().value());
        }
    }

    @Test
    void manualOffsetCommitRecoveryOnRejoin() throws Exception {
        server = startBroker();
        try (ProducerClient producer = new ProducerClient("127.0.0.1", server.boundPort())) {
            producer.connect();
            producer.createTopic("persist", 1);
            producer.publish("persist", null, "1".getBytes(StandardCharsets.UTF_8), Map.of());
            producer.publish("persist", null, "2".getBytes(StandardCharsets.UTF_8), Map.of());
        }
        ConsumerDeliverySettings settings =
                new ConsumerDeliverySettings(false, 0, "persist.dlq", true);
        try (GroupConsumerClient consumer = new GroupConsumerClient("127.0.0.1", server.boundPort())) {
            consumer.connect();
            consumer.joinGroup("g1", "c1", "persist");
            List<Message> firstBatch = consumer.poll(Duration.ofSeconds(2));
            assertEquals(2, firstBatch.size());
            consumer.commitOffsets(Map.of(0, firstBatch.getFirst().offset()));
        }
        try (GroupConsumerClient consumer = new GroupConsumerClient("127.0.0.1", server.boundPort())) {
            consumer.connect();
            consumer.joinGroup("g1", "c1", "persist");
            List<Message> secondBatch = consumer.poll(Duration.ofSeconds(2));
            assertEquals(1, secondBatch.size());
            assertEquals("2", new String(secondBatch.getFirst().value(), StandardCharsets.UTF_8));
        }
    }

    private static void assertArrayEquals(String expected, byte[] value) {
        assertEquals(expected, new String(value, StandardCharsets.UTF_8));
    }

    private static BrokerServer startBroker() throws IOException {
        int port;
        try (ServerSocket ephemeral = new ServerSocket(0)) {
            port = ephemeral.getLocalPort();
        }
        BrokerServer broker =
                new BrokerServer(
                        "127.0.0.1", port, 5_000, MAX_FRAME, 16, new BrokerRequestDispatcher(new BrokerServices()));
        broker.start();
        return broker;
    }
}
