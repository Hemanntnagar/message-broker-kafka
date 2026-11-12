package com.javastream.delivery;

/**
 * How long the broker waits before acknowledging a produce request.
 */
public enum ProducerAckLevel {
    /** Append to leader memory; no durability flush before response. */
    ACK_NONE(0),
    /** Leader has applied the configured flush policy (durable on single-node). */
    ACK_LEADER(1),
    /**
     * All in-sync replicas have the record. On a single broker this matches {@link #ACK_LEADER};
     * multi-broker replication must be enabled for a stronger guarantee.
     */
    ACK_ALL_REPLICAS(2);

    private final byte wireCode;

    ProducerAckLevel(int wireCode) {
        this.wireCode = (byte) wireCode;
    }

    public byte wireCode() {
        return wireCode;
    }

    public static ProducerAckLevel fromWire(byte code) {
        return switch (code) {
            case 0 -> ACK_NONE;
            case 1 -> ACK_LEADER;
            case 2 -> ACK_ALL_REPLICAS;
            default -> throw new IllegalArgumentException("Unknown producer ack level: " + code);
        };
    }
}
