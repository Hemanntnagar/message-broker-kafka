package com.javastream.admin;

import com.javastream.broker.BrokerServices;
import com.javastream.consumer.GroupConsumerClient;
import com.javastream.producer.ProducerClient;
import com.javastream.server.BrokerServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class BrokerAdminServiceTest {

    private BrokerServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void listTopicsDescribeGroupAndConsumerLag() throws Exception {
        BrokerServices services = new BrokerServices();
        server = startBroker(services);
        BrokerAdminService admin =
                new BrokerAdminService(
                        services, Optional.empty(), Instant.now(), "127.0.0.1", server.boundPort());

        try (ProducerClient producer = new ProducerClient("127.0.0.1", server.boundPort());
                GroupConsumerClient consumer = new GroupConsumerClient("127.0.0.1", server.boundPort())) {
            producer.connect();
            consumer.connect();
            producer.createTopic("admin-it", 1);
            producer.publish(
                    "admin-it",
                    "k".getBytes(StandardCharsets.UTF_8),
                    "v".getBytes(StandardCharsets.UTF_8),
                    java.util.Map.of());
            consumer.joinGroup("g1", "m1", "admin-it");
            consumer.commitOffsets(java.util.Map.of(0, 1L));

            assertFalse(admin.listTopics().isEmpty());
            assertEquals(1, admin.describeTopic(new com.javastream.topic.TopicName("admin-it")).partitionCount());
            assertEquals("admin-it", admin.describeGroup("g1").topic());
            assertEquals(0L, admin.getConsumerLag("g1", "admin-it").getFirst().lag());
        }
    }

    private BrokerServer startBroker(BrokerServices services) throws IOException {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        BrokerServer broker = BrokerServer.forLoadTest("127.0.0.1", port, services);
        broker.start();
        return broker;
    }
}
