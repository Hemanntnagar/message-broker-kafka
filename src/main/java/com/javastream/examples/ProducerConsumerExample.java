package com.javastream.examples;

import com.javastream.config.ConfigLoader;
import com.javastream.config.JavaStreamConfig;
import com.javastream.consumer.ConsumerClient;
import com.javastream.message.Message;
import com.javastream.producer.ProducerClient;
import com.javastream.producer.PublishAcknowledgement;
import com.javastream.server.JavaStreamServer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Minimal end-to-end producer and consumer demo against a local broker.
 */
public final class ProducerConsumerExample {

    private ProducerConsumerExample() {}

    public static void main(String[] args) throws Exception {
        JavaStreamConfig config = ConfigLoader.load();
        JavaStreamServer server = new JavaStreamServer(config);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));

        String host = config.listenHost().equals("0.0.0.0") ? "127.0.0.1" : config.listenHost();
        int port = config.listenPort();
        String topic = "demo";

        try (ProducerClient producer = new ProducerClient(host, port);
                ConsumerClient consumer = new ConsumerClient(host, port)) {
            producer.connect();
            consumer.connect();
            producer.createTopic(topic, 1);
            consumer.subscribe(topic);

            PublishAcknowledgement ack =
                    producer.publish(
                            topic,
                            "user-1".getBytes(StandardCharsets.UTF_8),
                            "hello-java-stream".getBytes(StandardCharsets.UTF_8),
                            Map.of("content-type", "text/plain".getBytes(StandardCharsets.UTF_8)));
            System.out.printf(
                    "Published messageId=%d partition=%d offset=%d%n",
                    ack.messageId(), ack.partition(), ack.offset());

            List<Message> messages = consumer.poll(Duration.ofSeconds(3));
            for (Message message : messages) {
                System.out.printf(
                        "Consumed offset=%d key=%s value=%s%n",
                        message.offset(),
                        message.key() == null ? "null" : new String(message.key(), StandardCharsets.UTF_8),
                        message.value() == null ? "null" : new String(message.value(), StandardCharsets.UTF_8));
                consumer.acknowledge(message);
            }
        }

        server.stop();
    }
}
