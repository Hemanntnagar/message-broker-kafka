package com.javastream.examples;

import com.javastream.consumer.ConsumerClient;
import com.javastream.message.Message;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/** Subscribes to a topic and prints polled records. */
public final class ConsumerExample {

    public static void main(String[] args) throws Exception {
        String host = ExampleBrokerTarget.host();
        int port = ExampleBrokerTarget.port();
        String topic = args.length > 0 ? args[0] : "example-producer";

        try (ConsumerClient consumer = new ConsumerClient(host, port)) {
            consumer.connect();
            consumer.subscribe(topic);
            List<Message> batch = consumer.poll(Duration.ofSeconds(5));
            for (Message message : batch) {
                System.out.printf(
                        "partition=%d offset=%d key=%s value=%s%n",
                        message.partition(),
                        message.offset(),
                        decode(message.key()),
                        decode(message.value()));
                consumer.acknowledge(message);
            }
            System.out.printf("Consumed %d message(s)%n", batch.size());
        }
    }

    private static String decode(byte[] bytes) {
        return bytes == null ? "null" : new String(bytes, StandardCharsets.UTF_8);
    }
}
