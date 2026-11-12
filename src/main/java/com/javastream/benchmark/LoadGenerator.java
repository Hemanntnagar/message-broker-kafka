package com.javastream.benchmark;

import com.javastream.consumer.ConsumerClient;
import com.javastream.producer.ProducerClient;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.client.TcpProtocolConnection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** End-to-end produce/consume load against a running broker. */
public final class LoadGenerator {

    private static final String TOPIC = "bench-throughput";

    public BenchmarkRunResult run(EmbeddedBenchmarkBroker broker, LoadScenario scenario) throws Exception {
        ResourceSampler startResources = ResourceSampler.capture();
        Instant startedAt = Instant.now();
        if (scenario.messagesPerProducer() > 0) {
            try (ProducerClient setup = new ProducerClient("127.0.0.1", broker.port())) {
                setup.connect();
                setup.createTopic(TOPIC, 1);
            }
        }

        long warmupEnd = System.nanoTime() + TimeUnit.SECONDS.toNanos(scenario.warmupSeconds());
        while (scenario.messagesPerProducer() > 0 && System.nanoTime() < warmupEnd) {
            try (ProducerClient warm = new ProducerClient("127.0.0.1", broker.port())) {
                warm.connect();
                warm.publish(TOPIC, "k".getBytes(StandardCharsets.UTF_8), payload(scenario.messageSizeBytes()), Map.of());
            }
        }

        List<Long> produceLatencies = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicLong produced = new AtomicLong();
        int producerCount = Math.max(0, scenario.producerCount());
        CountDownLatch producersDone = new CountDownLatch(producerCount);
        ExecutorService pool =
                Executors.newFixedThreadPool(Math.max(1, Math.min(producerCount, 128)));

        long testStart = System.nanoTime();
        for (int p = 0; p < producerCount; p++) {
            final int producerId = p;
            pool.submit(
                    () -> {
                        try {
                            Thread.sleep(producerId * 10L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        byte[] valueTemplate = payload(scenario.messageSizeBytes());
                        byte[] key = ("producer-" + producerId).getBytes(StandardCharsets.UTF_8);
                        try (ProducerClient client = new ProducerClient("127.0.0.1", broker.port())) {
                            client.connect();
                            int sent = 0;
                            while (sent < scenario.messagesPerProducer()) {
                                int batch = Math.min(scenario.produceBatchSize(), scenario.messagesPerProducer() - sent);
                                long t0 = System.nanoTime();
                                if (batch <= 1) {
                                    client.publish(TOPIC, key, valueTemplate, Map.of());
                                } else {
                                    List<RecordPayloadCodec.ProduceRecord> records = new ArrayList<>(batch);
                                    for (int i = 0; i < batch; i++) {
                                        records.add(new RecordPayloadCodec.ProduceRecord(key, valueTemplate, Map.of()));
                                    }
                                    client.publishBatch(TOPIC, 0, records);
                                }
                                produceLatencies.add(System.nanoTime() - t0);
                                sent += batch;
                                produced.addAndGet(batch);
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } finally {
                            producersDone.countDown();
                        }
                    });
        }

        if (producerCount > 0) {
            producersDone.await(10, TimeUnit.MINUTES);
        }
        long testEnd = System.nanoTime();
        pool.shutdownNow();

        int connections =
                probeConcurrentConnections(
                        broker.port(), scenario.connectionProbeCount(), scenario.holdConnectionsDuringProbe());

        long consumed = 0;
        List<Long> roundTrip = new ArrayList<>();
        if (scenario.messagesPerProducer() <= 0) {
            Instant endedAt = Instant.now();
            ResourceSampler endResources = ResourceSampler.capture();
            return new BenchmarkRunResult(
                    scenario,
                    startedAt,
                    endedAt,
                    0,
                    0,
                    0,
                    0,
                    new LatencyPercentiles(new long[0]),
                    new LatencyPercentiles(new long[0]),
                    startResources,
                    endResources,
                    connections,
                    "Connection capacity probe only");
        }
        try (ConsumerClient consumer = new ConsumerClient("127.0.0.1", broker.port())) {
            consumer.connect();
            consumer.subscribe(TOPIC);
            consumer.setBatchSize(scenario.fetchBatchSize());
            long expected = scenario.targetMessageCount();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (consumed < expected && System.nanoTime() < deadline) {
                long t0 = System.nanoTime();
                var batch = consumer.poll(Duration.ofMillis(500));
                if (!batch.isEmpty()) {
                    roundTrip.add(System.nanoTime() - t0);
                    consumed += batch.size();
                }
            }
        }

        Instant endedAt = Instant.now();
        ResourceSampler endResources = ResourceSampler.capture();
        double elapsedSec = (testEnd - testStart) / (double) TimeUnit.SECONDS.toNanos(1);
        double mps = produced.get() / elapsedSec;
        double mbps = (produced.get() * scenario.messageSizeBytes()) / (1024.0 * 1024.0) / elapsedSec;

        long[] produceArray = produceLatencies.stream().mapToLong(Long::longValue).toArray();
        long[] roundTripArray = roundTrip.stream().mapToLong(Long::longValue).toArray();

        return new BenchmarkRunResult(
                scenario,
                startedAt,
                endedAt,
                produced.get(),
                consumed,
                mps,
                mbps,
                new LatencyPercentiles(produceArray),
                new LatencyPercentiles(roundTripArray),
                startResources,
                endResources,
                connections,
                "");
    }

    private static int probeConcurrentConnections(int port, int target, boolean holdOpen) {
        if (target <= 0) {
            return 0;
        }
        int success = 0;
        List<TcpProtocolConnection> open = new ArrayList<>();
        try {
            for (int i = 0; i < target; i++) {
                TcpProtocolConnection conn = new TcpProtocolConnection("127.0.0.1", port, 2_000, 5_000, 65_536);
                try {
                    conn.connect();
                    conn.send(OperationCode.PING, new byte[0]);
                    success++;
                    if (holdOpen) {
                        open.add(conn);
                    } else {
                        conn.close();
                    }
                } catch (IOException | RuntimeException e) {
                    conn.close();
                    break;
                }
            }
        } finally {
            for (TcpProtocolConnection conn : open) {
                conn.close();
            }
        }
        return success;
    }

    private static byte[] payload(int size) {
        byte[] value = new byte[Math.max(1, size)];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) ('a' + (i % 26));
        }
        return value;
    }
}
