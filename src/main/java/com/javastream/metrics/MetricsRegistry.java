package com.javastream.metrics;

/** Hook for counters, gauges, and histograms; implemented by {@link BrokerMetrics}. */
public interface MetricsRegistry {

    void counter(String name, long delta);

    long counterValue(String name);
}
