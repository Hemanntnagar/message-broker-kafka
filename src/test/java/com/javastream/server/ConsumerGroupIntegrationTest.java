package com.javastream.server;

import com.javastream.broker.BrokerRequestDispatcher;
import com.javastream.broker.BrokerServices;
import com.javastream.config.JavaStreamConfig;
import com.javastream.consumer.GroupConsumerClient;
import com.javastream.message.Message;
import com.javastream.producer.ProducerClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerGroupIntegrationTest {

    private static final int MAX_FRAME = 65_536;

    private BrokerServer server;
    private BrokerServices services;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
        if (services != null) {
            services.close();
        }
    }

    @Test
    void singleGroupSingleConsumer() throws Exception {
        startBroker();
        publish("solo-topic", 1, 5);

        try (GroupConsumerClient consumer = new GroupConsumerClient("127.0.0.1", server.boundPort())) {
            consumer.connect();
            consumer.joinGroup("solo-group", "member-1", "solo-topic");
            assertEquals(List.of(0), consumer.assignedPartitions());

            List<Message> batch = consumer.poll(Duration.ofSeconds(2));
            assertEquals(5, batch.size());
            consumer.commitOffsets(Map.of(0, batch.getLast().offset()));

            Map<Integer, Long> committed = consumer.fetchCommittedOffsets(List.of(0));
            assertEquals(batch.getLast().offset(), committed.get(0));
        }
    }

    @Test
    void singleGroupMultipleConsumersSplitPartitions() throws Exception {
        startBroker();
        publish("shared-topic", 4, 4);

        try (GroupConsumerClient c1 = new GroupConsumerClient("127.0.0.1", server.boundPort());
                GroupConsumerClient c2 = new GroupConsumerClient("127.0.0.1", server.boundPort())) {
            c1.connect();
            c2.connect();
            c1.joinGroup("split-group", "consumer-1", "shared-topic");
            c2.joinGroup("split-group", "consumer-2", "shared-topic");
            c1.joinGroup("split-group", "consumer-1", "shared-topic");

            Set<Integer> owned = new HashSet<>();
            owned.addAll(c1.assignedPartitions());
            owned.addAll(c2.assignedPartitions());
            assertEquals(Set.of(0, 1, 2, 3), owned);
            assertTrue(c1.assignedPartitions().stream().noneMatch(c2.assignedPartitions()::contains));
        }
    }

    @Test
    void multipleGroupsSameTopicIndependentOffsets() throws Exception {
        startBroker();
        publish("dual-topic", 1, 2);

        try (GroupConsumerClient g1 = new GroupConsumerClient("127.0.0.1", server.boundPort());
                GroupConsumerClient g2 = new GroupConsumerClient("127.0.0.1", server.boundPort())) {
            g1.connect();
            g2.connect();
            g1.joinGroup("alpha", "a1", "dual-topic");
            g2.joinGroup("beta", "b1", "dual-topic");

            List<Message> batch1 = g1.poll(Duration.ofSeconds(2));
            g1.commitOffsets(Map.of(0, 0L));
            List<Message> batch2 = g2.poll(Duration.ofSeconds(2));
            g2.commitOffsets(Map.of(0, 1L));

            assertEquals(0L, g1.fetchCommittedOffsets(List.of(0)).get(0));
            assertEquals(1L, g2.fetchCommittedOffsets(List.of(0)).get(0));
            assertEquals(2, batch1.size());
            assertEquals(2, batch2.size());
        }
    }

    @Test
    void consumerDisconnectTriggersRebalance() throws Exception {
        startBroker();
        publish("rejoin-topic", 2, 2);

        GroupConsumerClient leaver = new GroupConsumerClient("127.0.0.1", server.boundPort());
        leaver.connect();
        leaver.joinGroup("rejoin-group", "leaver", "rejoin-topic");

        try (GroupConsumerClient survivor = new GroupConsumerClient("127.0.0.1", server.boundPort())) {
            survivor.connect();
            survivor.joinGroup("rejoin-group", "survivor", "rejoin-topic");
            assertEquals(1, survivor.assignedPartitions().size());

            leaver.leaveGroup();
            leaver.disconnect();

            survivor.joinGroup("rejoin-group", "survivor", "rejoin-topic");
            assertEquals(List.of(0, 1), survivor.assignedPartitions());
        }
    }

    @Test
    void committedOffsetsSurviveBrokerRestart() throws Exception {
        var dataDir = Files.createTempDirectory("javastream-group-offsets");
        int port;
        try (ServerSocket ephemeral = new ServerSocket(0)) {
            port = ephemeral.getLocalPort();
        }

        services = openFileBackedServices(dataDir);
        server = startBrokerOnPort(port, services);

        try (ProducerClient producer = new ProducerClient("127.0.0.1", server.boundPort());
                GroupConsumerClient consumer = new GroupConsumerClient("127.0.0.1", server.boundPort())) {
            producer.connect();
            consumer.connect();
            producer.createTopic("restart-topic", 1);
            producer.publish("restart-topic", null, "x".getBytes(StandardCharsets.UTF_8), Map.of());

            consumer.joinGroup("persist-group", "m1", "restart-topic");
            List<Message> batch = consumer.poll(Duration.ofSeconds(2));
            consumer.commitOffsets(Map.of(0, batch.getFirst().offset()));
        }

        server.stop();
        services.close();

        services = openFileBackedServices(dataDir);
        server = startBrokerOnPort(port, services);

        assertEquals(
                0L,
                services.consumerGroupManager()
                        .fetchCommittedOffsets("persist-group", "restart-topic", List.of(0))
                        .get(0));
    }

    @Test
    void concurrentGroupedConsumersConsumeAllPublishedRecords() throws Exception {
        startBroker();
        int partitionCount = 3;
        int messagesPerPartition = 5;
        try (ProducerClient producer = new ProducerClient("127.0.0.1", server.boundPort())) {
            producer.connect();
            producer.createTopic("load-topic", partitionCount);
            for (int partition = 0; partition < partitionCount; partition++) {
                for (int i = 0; i < messagesPerPartition; i++) {
                    producer.publish(
                            "load-topic",
                            String.valueOf(partition).getBytes(StandardCharsets.UTF_8),
                            ("p" + partition + "-m" + i).getBytes(StandardCharsets.UTF_8),
                            Map.of());
                }
            }
        }

        try (GroupConsumerClient c0 = new GroupConsumerClient("127.0.0.1", server.boundPort());
                GroupConsumerClient c1 = new GroupConsumerClient("127.0.0.1", server.boundPort());
                GroupConsumerClient c2 = new GroupConsumerClient("127.0.0.1", server.boundPort())) {
            c0.connect();
            c1.connect();
            c2.connect();
            c0.joinGroup("load-group", "member-0", "load-topic");
            c1.joinGroup("load-group", "member-1", "load-topic");
            c2.joinGroup("load-group", "member-2", "load-topic");
            c0.joinGroup("load-group", "member-0", "load-topic");
            c1.joinGroup("load-group", "member-1", "load-topic");
            c2.joinGroup("load-group", "member-2", "load-topic");

            List<Message> aggregated = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline && aggregated.size() < partitionCount * messagesPerPartition) {
                aggregated.addAll(c0.poll(Duration.ofMillis(100)));
                aggregated.addAll(c1.poll(Duration.ofMillis(100)));
                aggregated.addAll(c2.poll(Duration.ofMillis(100)));
            }
            assertEquals(partitionCount * messagesPerPartition, aggregated.size());
        }
    }

    private void startBroker() throws IOException {
        int port;
        try (ServerSocket ephemeral = new ServerSocket(0)) {
            port = ephemeral.getLocalPort();
        }
        services = new BrokerServices();
        server = startBrokerOnPort(port, services);

        try (ProducerClient setup = new ProducerClient("127.0.0.1", server.boundPort())) {
            // warm-up connection only
            setup.connect();
        }
    }

    private void publish(String topic, int partitions, int messages) throws IOException {
        try (ProducerClient producer = new ProducerClient("127.0.0.1", server.boundPort())) {
            producer.connect();
            producer.createTopic(topic, partitions);
            for (int i = 0; i < messages; i++) {
                producer.publish(topic, null, ("m-" + i).getBytes(StandardCharsets.UTF_8), Map.of());
            }
        }
    }

    private static BrokerServer startBrokerOnPort(int port, BrokerServices brokerServices) {
        BrokerServer broker =
                new BrokerServer(
                        "127.0.0.1",
                        port,
                        5_000,
                        MAX_FRAME,
                        16,
                        new BrokerRequestDispatcher(brokerServices));
        broker.start();
        return broker;
    }

    private static BrokerServices openFileBackedServices(java.nio.file.Path dataDir) throws Exception {
        JavaStreamConfig config = new JavaStreamConfig("test", 1, "127.0.0.1", 9092, dataDir.toString(), false, 1);
        return BrokerServices.create(config);
    }
}
