package com.javastream.metrics;

/**
 * Hook for counters, gauges, and histograms (Micrometer or JMX backends later).
 */
public interface MetricsRegistry {

    void counter(String name, long delta);

    long counterValue(String name);
}
