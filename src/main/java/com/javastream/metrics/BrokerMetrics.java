package com.javastream.metrics;

import com.javastream.broker.BrokerServices;
import com.javastream.cluster.ClusterManager;
import com.javastream.partition.PartitionId;
import com.javastream.replication.ClusterReplicaManager;
import com.javastream.topic.TopicName;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;

/**
 * Broker-scoped Micrometer meters for produce/fetch rates, lag, and resource usage.
 */
public final class BrokerMetrics implements MetricsRegistry, AutoCloseable {

    private final MeterRegistry registry;
    private final int brokerId;
    private final Instant startedAt;
    private volatile IntSupplier activeConnections = () -> 0;
    private final AtomicLong activeProducers = new AtomicLong();
    private final AtomicLong activeConsumers = new AtomicLong();
    private final LongAdder published = new LongAdder();
    private final LongAdder consumed = new LongAdder();
    private final LongAdder recentPublished = new LongAdder();
    private volatile BrokerServices services;
    private volatile Optional<ClusterManager> clusterManager = Optional.empty();
    private volatile Path dataDirectory = Path.of(".");
    private volatile boolean closed;

    public BrokerMetrics(int brokerId) {
        this(new SimpleMeterRegistry(), brokerId, Instant.now());
    }

    public BrokerMetrics(MeterRegistry registry, int brokerId, Instant startedAt) {
        this.registry = registry;
        this.brokerId = brokerId;
        this.startedAt = startedAt;
        new JvmMemoryMetrics().bindTo(registry);

        Counter.builder("javastream.messages.published")
                .description("Total messages published")
                .register(registry);
        Counter.builder("javastream.messages.consumed")
                .description("Total messages consumed")
                .register(registry);
        Counter.builder("javastream.requests.failed")
                .description("Broker requests that returned a non-OK status")
                .register(registry);

        Gauge.builder("javastream.messages.published.total", published, LongAdder::sum)
                .description("Total messages published")
                .register(registry);
        Gauge.builder("javastream.messages.consumed.total", consumed, LongAdder::sum)
                .description("Total messages consumed")
                .register(registry);
        Gauge.builder("javastream.messages.per.second", recentPublished, this::messagesPerSecondEstimate)
                .description("Estimated publish rate over the last minute")
                .register(registry);
        Gauge.builder("javastream.connections.active", this, BrokerMetrics::activeConnectionCount)
                .description("Active client TCP connections")
                .register(registry);
        Gauge.builder("javastream.producers.active", activeProducers, AtomicLong::get)
                .description("Connections that have published at least once")
                .register(registry);
        Gauge.builder("javastream.consumers.active", activeConsumers, AtomicLong::get)
                .description("Connections that have consumed or joined a group")
                .register(registry);
        Gauge.builder("javastream.topics.count", this, BrokerMetrics::topicCount)
                .description("Registered topics")
                .register(registry);
        Gauge.builder("javastream.partitions.count", this, BrokerMetrics::partitionCount)
                .description("Total topic partitions")
                .register(registry);
        Gauge.builder("javastream.consumer.lag.max", this, BrokerMetrics::maxConsumerLag)
                .description("Maximum consumer lag across committed offsets")
                .register(registry);
        Gauge.builder("javastream.disk.usage.bytes", this, BrokerMetrics::diskUsageBytes)
                .description("Used bytes on the data directory file store")
                .register(registry);
        Gauge.builder("javastream.memory.used.bytes", this, BrokerMetrics::heapUsedBytes)
                .description("JVM heap used bytes")
                .register(registry);
        Gauge.builder("javastream.replication.lag.max", this, BrokerMetrics::maxReplicationLag)
                .description("Maximum leader-follower log end offset gap")
                .register(registry);
        Gauge.builder("javastream.broker.uptime.seconds", this, BrokerMetrics::uptimeSeconds)
                .description("Broker process uptime")
                .register(registry);
        Gauge.builder("javastream.broker.id", this, BrokerMetrics::brokerIdValue)
                .strongReference(true)
                .register(registry);
    }

    public MeterRegistry meterRegistry() {
        return registry;
    }

    public void setActiveConnections(IntSupplier activeConnections) {
        this.activeConnections = activeConnections == null ? () -> 0 : activeConnections;
    }

    private int activeConnectionCount() {
        return activeConnections.getAsInt();
    }

    public void attach(BrokerServices services, Path dataDirectory, Optional<ClusterManager> clusterManager) {
        this.services = services;
        this.dataDirectory = dataDirectory == null ? Path.of(".") : dataDirectory;
        this.clusterManager = clusterManager == null ? Optional.empty() : clusterManager;
    }

    public void recordPublished(int count) {
        if (count <= 0) {
            return;
        }
        published.add(count);
        recentPublished.add(count);
        registry.counter("javastream.messages.published").increment(count);
    }

    public void recordConsumed(int count) {
        if (count <= 0) {
            return;
        }
        consumed.add(count);
        registry.counter("javastream.messages.consumed").increment(count);
    }

    public void recordFailedRequest() {
        registry.counter("javastream.requests.failed").increment();
    }

    public void markProducerActive() {
        activeProducers.incrementAndGet();
    }

    public void markConsumerActive() {
        activeConsumers.incrementAndGet();
    }

    public void resetRates() {
        recentPublished.reset();
    }

    @Override
    public void counter(String name, long delta) {
        registry.counter(name).increment(delta);
    }

    @Override
    public long counterValue(String name) {
        Counter counter = registry.find(name).counter();
        return counter == null ? 0L : (long) counter.count();
    }

    @Override
    public void close() {
        closed = true;
        if (registry instanceof SimpleMeterRegistry simple) {
            simple.close();
        }
    }

    private double messagesPerSecondEstimate(LongAdder recent) {
        long count = recent.sum();
        if (count == 0) {
            return 0.0;
        }
        double seconds = Math.max(1.0, Duration.between(startedAt, Instant.now()).toMillis() / 1000.0);
        return count / seconds;
    }

    private int topicCount() {
        BrokerServices svc = services;
        return svc == null ? 0 : svc.listTopics().size();
    }

    private int partitionCount() {
        BrokerServices svc = services;
        if (svc == null) {
            return 0;
        }
        int total = 0;
        for (String topic : svc.listTopics()) {
            total += svc.partitionCount(new TopicName(topic)).orElse(0);
        }
        return total;
    }

    private long maxConsumerLag() {
        BrokerServices svc = services;
        if (svc == null) {
            return 0;
        }
        long maxLag = 0;
        for (var entry : svc.consumerGroupManager().committedOffsetsSnapshot().entrySet()) {
            long committed = entry.getValue();
            if (committed < 0) {
                continue;
            }
            TopicName topic = new TopicName(entry.getKey().topic());
            int partition = entry.getKey().partition();
            long logEnd =
                    svc.topicMetadata(topic)
                            .map(meta -> meta.partitions().get(partition).logEndOffset())
                            .orElse(0L);
            maxLag = Math.max(maxLag, Math.max(0, logEnd - committed));
        }
        return maxLag;
    }

    private long diskUsageBytes() {
        try {
            Path dir = dataDirectory.toAbsolutePath().normalize();
            Files.createDirectories(dir);
            FileStore store = Files.getFileStore(dir);
            return store.getTotalSpace() - store.getUnallocatedSpace();
        } catch (IOException | RuntimeException e) {
            return 0L;
        }
    }

    private long heapUsedBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private long maxReplicationLag() {
        BrokerServices svc = services;
        if (svc == null) {
            return 0;
        }
        Optional<ClusterReplicaManager> replicaManager = svc.clusterReplicaManager();
        if (replicaManager.isEmpty()) {
            return 0;
        }
        ClusterReplicaManager manager = replicaManager.get();
        long maxLag = 0;
        for (String topic : svc.listTopics()) {
            TopicName topicName = new TopicName(topic);
            int partitions = svc.partitionCount(topicName).orElse(0);
            for (int p = 0; p < partitions; p++) {
                PartitionId id = new PartitionId(topicName, p);
                int partitionIndex = p;
                long leaderEnd =
                        svc.topicMetadata(topicName)
                                .map(meta -> meta.partitions().get(partitionIndex).logEndOffset())
                                .orElse(0L);
                long hwm = manager.highWatermark(id);
                maxLag = Math.max(maxLag, Math.max(0, leaderEnd - hwm));
            }
        }
        return maxLag;
    }

    private double uptimeSeconds() {
        return Duration.between(startedAt, Instant.now()).toMillis() / 1000.0;
    }

    private double brokerIdValue() {
        return brokerId;
    }
}
