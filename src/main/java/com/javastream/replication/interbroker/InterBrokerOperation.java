package com.javastream.replication.interbroker;

/** Inter-broker RPC operation codes (separate TCP listener from client protocol). */
public enum InterBrokerOperation {
    BROKER_REGISTER(1),
    BROKER_HEARTBEAT(2),
    REPLICATE_RECORD(3),
    REPLICA_FETCH(4),
    TOPIC_CREATED(5),
    ASSIGNMENT_PUSH(6);

    private final short wire;

    InterBrokerOperation(int wire) {
        this.wire = (short) wire;
    }

    public short wire() {
        return wire;
    }

    public static InterBrokerOperation fromWire(short value) {
        for (InterBrokerOperation op : values()) {
            if (op.wire == value) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown inter-broker operation: " + (value & 0xFFFF));
    }
}
