package com.javastream.examples;

import com.javastream.producer.ProducerClient;
import com.javastream.producer.PublishAcknowledgement;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Publishes sample records to a running broker. */
public final class ProducerExample {

    public static void main(String[] args) throws Exception {
        String host = ExampleBrokerTarget.host();
        int port = ExampleBrokerTarget.port();
        String topic = args.length > 0 ? args[0] : "example-producer";

        try (ProducerClient producer = new ProducerClient(host, port)) {
            producer.connect();
            producer.createTopic(topic, 2);
            for (int i = 0; i < 5; i++) {
                PublishAcknowledgement ack =
                        producer.publish(
                                topic,
                                ("key-" + i).getBytes(StandardCharsets.UTF_8),
                                ("value-" + i).getBytes(StandardCharsets.UTF_8),
                                Map.of());
                System.out.printf(
                        "published i=%d partition=%d offset=%d%n", i, ack.partition(), ack.offset());
            }
        }
    }
}
