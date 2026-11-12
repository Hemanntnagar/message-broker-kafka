package com.javastream.examples;

import com.javastream.consumer.GroupConsumerClient;
import com.javastream.message.Message;
import com.javastream.producer.ProducerClient;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/** Consumer group join, poll, and offset commit. */
public final class ConsumerGroupExample {

    public static void main(String[] args) throws Exception {
        String host = ExampleBrokerTarget.host();
        int port = ExampleBrokerTarget.port();
        String topic = "example-consumer-group";
        String groupId = "demo-group";

        try (ProducerClient producer = new ProducerClient(host, port)) {
            producer.connect();
            producer.createTopic(topic, 2);
            for (int i = 0; i < 8; i++) {
                producer.publish(
                        topic,
                        null,
                        ("event-" + i).getBytes(StandardCharsets.UTF_8),
                        Map.of());
            }
        }

        try (GroupConsumerClient consumer = new GroupConsumerClient(host, port)) {
            consumer.connect();
            consumer.joinGroup(groupId, "member-1", topic);
            System.out.println("Assigned partitions: " + consumer.assignedPartitions());

            List<Message> batch = consumer.poll(Duration.ofSeconds(5));
            for (Message message : batch) {
                System.out.printf(
                        "partition=%d offset=%d value=%s%n",
                        message.partition(),
                        message.offset(),
                        new String(message.value(), StandardCharsets.UTF_8));
            }
            if (!batch.isEmpty()) {
                Map<Integer, Long> commits = Map.of(batch.getLast().partition(), batch.getLast().offset());
                consumer.commitOffsets(commits);
                System.out.println("Committed offsets: " + commits);
            }
            consumer.leaveGroup();
        }
    }
}
