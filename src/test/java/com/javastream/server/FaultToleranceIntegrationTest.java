package com.javastream.server;

import com.javastream.broker.ClusterBrokerNode;
import com.javastream.client.FailureInjectingConnection;
import com.javastream.client.TcpProtocolConnection;
import com.javastream.delivery.ProducerDeliverySettings;
import com.javastream.producer.PublishAcknowledgement;
import com.javastream.producer.ReliableProducerClient;
import com.javastream.config.JavaStreamConfig;
import com.javastream.delivery.ProducerAckLevel;
import com.javastream.partition.PartitionId;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.Response;
import com.javastream.topic.TopicName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FaultToleranceIntegrationTest {

    private final List<ClusterBrokerNode> brokers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (ClusterBrokerNode broker : brokers) {
            broker.stop();
        }
        brokers.clear();
    }

    @Test
    void leaderFailoverIncrementsEpochAndAcceptsWritesOnNewLeader() throws Exception {
        ClusterBrokerNode coordinator = startBroker(clusterConfig(0, 0, 2, 2_000, 300));
        int repl = coordinator.runtime().interBrokerServer().boundPort();
        ClusterBrokerNode broker1 = startBroker(clusterConfig(1, repl, 2, 2_000, 300));
        ClusterBrokerNode broker2 = startBroker(clusterConfig(2, repl, 2, 2_000, 300));
        Thread.sleep(600);
        createTopic(coordinator, "ft", 2);
        Thread.sleep(200);
        PartitionId partition = new PartitionId(new TopicName("ft"), 1);
        int initialLeader =
                coordinator.runtime().clusterManager().assignment(partition).orElseThrow().leaderId();
        ClusterBrokerNode leaderNode = initialLeader == 0 ? coordinator : (initialLeader == 1 ? broker1 : broker2);
        produce(leaderNode, "ft", 1, "before".getBytes(StandardCharsets.UTF_8));

        long epochBefore =
                coordinator.runtime().clusterManager().assignment(partition).orElseThrow().leaderEpoch();

        leaderNode.stop();
        brokers.remove(leaderNode);
        Thread.sleep(4_500);

        var after = coordinator.runtime().clusterManager().assignment(partition).orElseThrow();
        assertTrue(after.leaderEpoch() > epochBefore);
        ClusterBrokerNode newLeader =
                after.leaderId() == 0 ? coordinator : (after.leaderId() == 1 ? broker1 : broker2);
        produce(newLeader, "ft", 1, "after".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void interruptedProduceRetriesAfterNetworkFailure() throws Exception {
        ClusterBrokerNode broker = startBroker(singleBrokerConfig());
        int port = broker.brokerServer().boundPort();
        TcpProtocolConnection raw = new TcpProtocolConnection("127.0.0.1", port);
        FailureInjectingConnection flaky = new FailureInjectingConnection(raw, 1);
        ProducerDeliverySettings settings =
                ProducerDeliverySettings.builder()
                        .maxRetries(2)
                        .retryBackoff(Duration.ofMillis(10))
                        .requestTimeout(Duration.ofSeconds(5))
                        .build();
        try (ReliableProducerClient producer = new ReliableProducerClient(flaky, settings)) {
            producer.connect();
            producer.createTopic("retry", 1);
            PublishAcknowledgement ack =
                    producer.publish("retry", null, "msg".getBytes(StandardCharsets.UTF_8), Map.of());
            assertEquals(0, ack.offset());
        }
    }

    private ClusterBrokerNode startBroker(JavaStreamConfig config) {
        ClusterBrokerNode node = new ClusterBrokerNode(config);
        node.start();
        brokers.add(node);
        return node;
    }

    private static JavaStreamConfig clusterConfig(
            int brokerId, int coordinatorReplicationPort, int rf, long sessionMs, long heartbeatMs) {
        return new JavaStreamConfig(
                "test",
                brokerId,
                "127.0.0.1",
                0,
                "./data-ft-" + brokerId,
                true,
                1,
                0,
                "127.0.0.1",
                coordinatorReplicationPort,
                0,
                rf,
                sessionMs,
                heartbeatMs);
    }

    private static JavaStreamConfig singleBrokerConfig() {
        return new JavaStreamConfig("test", 0, "127.0.0.1", 0, "./data-ft-single", false, 1);
    }

    private static void createTopic(ClusterBrokerNode broker, String topic, int partitions) throws Exception {
        try (TcpProtocolConnection connection = new TcpProtocolConnection("127.0.0.1", broker.brokerServer().boundPort())) {
            connection.connect();
            Response response =
                    connection.send(
                            OperationCode.CREATE_TOPIC,
                            RecordPayloadCodec.encodeCreateTopicRequest(
                                    new RecordPayloadCodec.CreateTopicRequest(topic, partitions)),
                            Duration.ofSeconds(5));
            assertEquals("OK", response.status().name());
        }
    }

    private static void produce(ClusterBrokerNode broker, String topic, int partition, byte[] value) throws Exception {
        try (TcpProtocolConnection connection = new TcpProtocolConnection("127.0.0.1", broker.brokerServer().boundPort())) {
            connection.connect();
            RecordPayloadCodec.ProduceRequest request =
                    new RecordPayloadCodec.ProduceRequest(
                            topic,
                            partition,
                            null,
                            value,
                            Map.of(),
                            ProducerAckLevel.ACK_ALL_REPLICAS,
                            5_000,
                            -1,
                            0);
            Response response =
                    connection.send(
                            OperationCode.PRODUCE,
                            RecordPayloadCodec.encodeProduceRequest(request),
                            Duration.ofSeconds(15));
            assertEquals("OK", response.status().name());
        }
    }
}
