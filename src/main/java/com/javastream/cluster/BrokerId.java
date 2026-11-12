package com.javastream.cluster;

/**
 * Unique identifier for a broker instance in a cluster.
 */
public record BrokerId(int value) {

    public BrokerId {
        if (value < 0) {
            throw new IllegalArgumentException("broker id must be non-negative");
        }
    }
}
