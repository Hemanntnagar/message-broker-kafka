package com.javastream.server;

import com.javastream.consumer.ConsumerClient;
import com.javastream.exception.BrokerResponseException;
import com.javastream.exception.ClientNotConnectedException;
import com.javastream.message.Message;
import com.javastream.producer.ProducerClient;
import com.javastream.producer.PublishAcknowledgement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerConsumerIntegrationTest {

    private static final int MAX_FRAME = 65_536;

    private BrokerServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void publishSingleMessageAndConsume() throws Exception {
        server = startBroker();
        try (ProducerClient producer = new ProducerClient("127.0.0.1", server.boundPort());
                ConsumerClient consumer = new ConsumerClient("127.0.0.1", server.boundPort())) {
            producer.connect();
            consumer.connect();

            producer.createTopic("orders", 1);
            byte[] key = "k1".getBytes(StandardCharsets.UTF_8);
            byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
            Map<String, byte[]> headers = Map.of("trace", "abc".getBytes(StandardCharsets.UTF_8));

            PublishAcknowledgement ack = producer.publish("orders", key, value, headers);
            assertEquals("orders", ack.topic());
            assertEquals(0, ack.partition());
            assertEquals(0, ack.offset());
            assertTrue(ack.messageId() > 0);

            consumer.subscribe("orders");
            List<Message> batch = consumer.poll(Duration.ofSeconds(2));
            assertEquals(1, batch.size());
            Message message = batch.getFirst();
            assertArrayEquals(key, message.key());
            assertArrayEquals(value, message.value());
            assertArrayEquals(headers.get("trace"), message.headers().get("trace"));
            assertEquals(ack.messageId(), message.messageId());
            assertEquals(ack.offset(), message.offset());
            consumer.acknowledge(message);
        }
    }

    @Test
    void publishMultipleMessagesInOrder() throws Exception {
        server = startBroker();
        try (ProducerClient producer = new ProducerClient("127.0.0.1", server.boundPort());
                ConsumerClient consumer = new ConsumerClient("127.0.0.1", server.boundPort())) {
            producer.connect();
            consumer.connect();
            producer.createTopic("events", 1);

            for (int i = 0; i < 5; i++) {
                producer.publish(
                        "events",
                        null,
                        ("m-" + i).getBytes(StandardCharsets.UTF_8),
                        Map.of());
            }

            consumer.subscribe("events");
            consumer.setBatchSize(10);
            List<Message> messages = consumer.poll(Duration.ofSeconds(2));
            assertEquals(5, messages.size());
            for (int i = 0; i < 5; i++) {
                assertEquals(i, messages.get(i).offset());
                assertArrayEquals(("m-" + i).getBytes(StandardCharsets.UTF_8), messages.get(i).value());
            }
        }
    }

    @Test
    void invalidTopicReturnsNotFound() throws Exception {
        server = startBroker();
        try (ProducerClient producer = new ProducerClient("127.0.0.1", server.boundPort())) {
            producer.connect();
            BrokerResponseException error =
                    assertThrows(
                            BrokerResponseException.class,
                            () -> producer.publish("missing", null, new byte[] {1}, Map.of()));
            assertEquals(com.javastream.protocol.StatusCode.NOT_FOUND, error.status());
        }
    }

    @Test
    void disconnectedProducerCannotPublish() {
        ProducerClient producer = new ProducerClient("127.0.0.1", 1);
        assertThrows(ClientNotConnectedException.class, () -> producer.publish("t", null, new byte[] {1}, Map.of()));
    }

    @Test
    void pollTimeoutReturnsEmptyBatch() throws Exception {
        server = startBroker();
        try (ProducerClient producer = new ProducerClient("127.0.0.1", server.boundPort());
                ConsumerClient consumer = new ConsumerClient("127.0.0.1", server.boundPort())) {
            producer.connect();
            consumer.connect();
            producer.createTopic("idle", 1);
            consumer.subscribe("idle");
            consumer.setBatchSize(3);
            List<Message> empty = consumer.poll(Duration.ofMillis(200));
            assertTrue(empty.isEmpty());
        }
    }

    @Test
    void concurrentPublishersAndConsumers() throws Exception {
        server = startBroker();
        int publishers = 4;
        int messagesPerPublisher = 25;

        try (ProducerClient setup = new ProducerClient("127.0.0.1", server.boundPort())) {
            setup.connect();
            setup.createTopic("shared", 1);
        }

        CountDownLatch published = new CountDownLatch(publishers);
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int p = 0; p < publishers; p++) {
                final int publisherId = p;
                pool.submit(
                        () -> {
                            try (ProducerClient producer =
                                    new ProducerClient("127.0.0.1", server.boundPort())) {
                                producer.connect();
                                for (int i = 0; i < messagesPerPublisher; i++) {
                                    String payload = publisherId + ":" + i;
                                    producer.publish(
                                            "shared",
                                            String.valueOf(publisherId).getBytes(StandardCharsets.UTF_8),
                                            payload.getBytes(StandardCharsets.UTF_8),
                                            Map.of());
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            } finally {
                                published.countDown();
                            }
                        });
            }
            assertTrue(published.await(15, TimeUnit.SECONDS));
        }

        List<Message> consumed = new ArrayList<>();
        try (ConsumerClient consumer = new ConsumerClient("127.0.0.1", server.boundPort())) {
            consumer.connect();
            consumer.subscribe("shared");
            consumer.setBatchSize(50);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (consumed.size() < publishers * messagesPerPublisher && System.nanoTime() < deadline) {
                consumed.addAll(consumer.poll(Duration.ofMillis(300)));
            }
        }

        assertEquals(publishers * messagesPerPublisher, consumed.size());

        try (ConsumerClient first = new ConsumerClient("127.0.0.1", server.boundPort());
                ConsumerClient second = new ConsumerClient("127.0.0.1", server.boundPort())) {
            first.connect();
            second.connect();
            first.subscribe("shared");
            second.subscribe("shared");
            first.setBatchSize(100);
            second.setBatchSize(100);
            assertEquals(publishers * messagesPerPublisher, first.poll(Duration.ofSeconds(2)).size());
            assertEquals(publishers * messagesPerPublisher, second.poll(Duration.ofSeconds(2)).size());
        }
    }

    private static BrokerServer startBroker() throws IOException {
        int port;
        try (ServerSocket ephemeral = new ServerSocket(0)) {
            port = ephemeral.getLocalPort();
        }
        BrokerServer broker = new BrokerServer("127.0.0.1", port, 5_000, MAX_FRAME, 16);
        broker.start();
        return broker;
    }
}
