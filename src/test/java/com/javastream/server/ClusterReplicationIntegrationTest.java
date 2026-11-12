package com.javastream.server;

import com.javastream.broker.ClusterBrokerNode;
import com.javastream.client.TcpProtocolConnection;
import com.javastream.config.JavaStreamConfig;
import com.javastream.delivery.ProducerAckLevel;
import com.javastream.partition.PartitionId;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.Response;
import com.javastream.cluster.PartitionReplicaAssignment;
import com.javastream.topic.TopicName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterReplicationIntegrationTest {

    private final List<ClusterBrokerNode> brokers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (ClusterBrokerNode broker : brokers) {
            broker.stop();
        }
        brokers.clear();
    }

    @Test
    void threeBrokerReplicationFollowerRecoveryAndLeaderFailover() throws Exception {
        ClusterBrokerNode coordinator = startBroker(clusterConfig(0, 0, 2, 3_000, 400));
        int coordinatorReplicationPort = coordinator.runtime().interBrokerServer().boundPort();
        ClusterBrokerNode broker1 =
                startBroker(clusterConfig(1, coordinatorReplicationPort, 2, 3_000, 400));
        ClusterBrokerNode broker2 =
                startBroker(clusterConfig(2, coordinatorReplicationPort, 2, 3_000, 400));

        Thread.sleep(800);

        createTopic(coordinator, "events", 3);
        Thread.sleep(300);

        produce(coordinator, "events", 0, "m0".getBytes(StandardCharsets.UTF_8));
        produce(broker1, "events", 1, "m1".getBytes(StandardCharsets.UTF_8));

        assertRecordPresent(broker1, "events", 0, 0, "m0");
        assertRecordPresent(broker2, "events", 1, 0, "m1");

        broker1.stop();
        brokers.remove(broker1);
        produce(coordinator, "events", 0, "m-after-stop".getBytes(StandardCharsets.UTF_8));

        ClusterBrokerNode restarted =
                startBroker(clusterConfig(1, coordinatorReplicationPort, 2, 3_000, 400));
        Thread.sleep(1_200);
        restarted.runtime().replicationEngine().catchUpAllAssignedFollowers();
        assertRecordPresent(restarted, "events", 0, 1, "m-after-stop");

        PartitionId partitionOne = new PartitionId(new TopicName("events"), 1);
        Optional<PartitionReplicaAssignment> beforeSecondStop =
                coordinator.runtime().clusterManager().assignment(partitionOne);
        assertTrue(beforeSecondStop.isPresent());
        assertEquals(1, beforeSecondStop.get().leaderId());

        broker1 = restarted;
        broker1.stop();
        brokers.remove(broker1);
        Thread.sleep(4_500);

        Optional<PartitionReplicaAssignment> afterFailover =
                coordinator.runtime().clusterManager().assignment(partitionOne);
        assertTrue(afterFailover.isPresent());
        assertFalse(afterFailover.get().leaderId() == 1, "partition 1 leader should fail over");

        int newLeaderId = afterFailover.get().leaderId();
        ClusterBrokerNode newLeader = broker2;
        if (newLeaderId == 0) {
            newLeader = coordinator;
        }
        produce(newLeader, "events", 1, "post-failover".getBytes(StandardCharsets.UTF_8));
        assertRecordPresent(newLeader, "events", 1, 1, "post-failover");
    }

    private ClusterBrokerNode startBroker(JavaStreamConfig config) {
        ClusterBrokerNode node = new ClusterBrokerNode(config);
        node.start();
        brokers.add(node);
        return node;
    }

    private static JavaStreamConfig clusterConfig(
            int brokerId,
            int coordinatorReplicationPort,
            int replicationFactor,
            long sessionTimeoutMs,
            long heartbeatMs) {
        return new JavaStreamConfig(
                "test",
                brokerId,
                "127.0.0.1",
                0,
                "./data-test-" + brokerId,
                true,
                1,
                0,
                "127.0.0.1",
                coordinatorReplicationPort,
                0,
                replicationFactor,
                sessionTimeoutMs,
                heartbeatMs);
    }

    private static void createTopic(ClusterBrokerNode broker, String topic, int partitions) throws Exception {
        try (TcpProtocolConnection connection = new TcpProtocolConnection("127.0.0.1", broker.brokerServer().boundPort())) {
            connection.connect();
            RecordPayloadCodec.CreateTopicRequest request = new RecordPayloadCodec.CreateTopicRequest(topic, partitions);
            Response response =
                    connection.send(
                            OperationCode.CREATE_TOPIC,
                            RecordPayloadCodec.encodeCreateTopicRequest(request),
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
                            Duration.ofSeconds(10));
            assertEquals("OK", response.status().name());
        }
    }

    private static void assertRecordPresent(
            ClusterBrokerNode broker, String topic, int partition, long offset, String expectedValue)
            throws Exception {
        try (TcpProtocolConnection connection = new TcpProtocolConnection("127.0.0.1", broker.brokerServer().boundPort())) {
            connection.connect();
            RecordPayloadCodec.FetchRequest fetch =
                    new RecordPayloadCodec.FetchRequest(topic, partition, offset, 10, 2_000);
            Response response =
                    connection.send(
                            OperationCode.FETCH,
                            RecordPayloadCodec.encodeFetchRequest(fetch),
                            Duration.ofSeconds(5));
            assertEquals("OK", response.status().name());
            RecordPayloadCodec.FetchResponse decoded =
                    RecordPayloadCodec.decodeFetchResponse(response.payload());
            assertFalse(decoded.records().isEmpty(), "expected record at offset " + offset);
            assertArrayEquals(expectedValue.getBytes(StandardCharsets.UTF_8), decoded.records().getFirst().value());
        }
    }
}
