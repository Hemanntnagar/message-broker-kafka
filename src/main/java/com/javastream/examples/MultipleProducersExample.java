package com.javastream.examples;

import com.javastream.producer.ProducerClient;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Several producers publishing concurrently to the same topic. */
public final class MultipleProducersExample {

    public static void main(String[] args) throws Exception {
        String host = ExampleBrokerTarget.host();
        int port = ExampleBrokerTarget.port();
        String topic = "example-multi-producers";
        int producers = args.length > 0 ? Integer.parseInt(args[0]) : 3;

        try (ProducerClient bootstrap = new ProducerClient(host, port)) {
            bootstrap.connect();
            bootstrap.createTopic(topic, producers);
        }

        try (ExecutorService pool = Executors.newFixedThreadPool(producers)) {
            List<Callable<Integer>> tasks = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                final int producerId = p;
                tasks.add(
                        () -> {
                            try (ProducerClient client = new ProducerClient(host, port)) {
                                client.connect();
                                int count = 0;
                                for (int i = 0; i < 10; i++) {
                                    client.publish(
                                            topic,
                                            ("p" + producerId).getBytes(StandardCharsets.UTF_8),
                                            ("m-" + producerId + "-" + i).getBytes(StandardCharsets.UTF_8),
                                            java.util.Map.of());
                                    count++;
                                }
                                return count;
                            }
                        });
            }
            int total = 0;
            for (Future<Integer> future : pool.invokeAll(tasks)) {
                total += future.get();
            }
            System.out.printf("Published %d messages from %d producers%n", total, producers);
        }
    }
}
