package com.javastream.examples;

import com.javastream.consumer.ConsumerClient;
import com.javastream.message.Message;
import com.javastream.producer.ProducerClient;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/** Independent consumers, each pinned to a different partition. */
public final class MultipleConsumersExample {

    public static void main(String[] args) throws Exception {
        String host = ExampleBrokerTarget.host();
        int port = ExampleBrokerTarget.port();
        String topic = "example-multi-consumers";
        int partitions = 3;

        try (ProducerClient producer = new ProducerClient(host, port)) {
            producer.connect();
            producer.createTopic(topic, partitions);
            for (int i = 0; i < partitions; i++) {
                producer.publish(
                        topic,
                        i,
                        null,
                        ("seed-partition-" + i).getBytes(StandardCharsets.UTF_8),
                        java.util.Map.of());
            }
        }

        AtomicInteger total = new AtomicInteger();
        try (ExecutorService pool = Executors.newFixedThreadPool(partitions)) {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int partition = 0; partition < partitions; partition++) {
                final int p = partition;
                tasks.add(
                        () -> {
                            try (ConsumerClient consumer = new ConsumerClient(host, port)) {
                                consumer.connect();
                                consumer.subscribe(topic);
                                consumer.setPartition(p);
                                List<Message> batch = consumer.poll(Duration.ofSeconds(5));
                                for (Message message : batch) {
                                    System.out.printf(
                                            "consumer-p%d offset=%d value=%s%n",
                                            p,
                                            message.offset(),
                                            new String(message.value(), StandardCharsets.UTF_8));
                                    total.incrementAndGet();
                                }
                            }
                            return null;
                        });
            }
            for (Future<Void> future : pool.invokeAll(tasks)) {
                future.get();
            }
        }
        System.out.printf("Read %d message(s) across %d consumers%n", total.get(), partitions);
    }
}
