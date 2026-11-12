package com.javastream.replication.interbroker;

/** Framing constants for inter-broker TCP RPCs (not the client JSTR protocol). */
public final class InterBrokerConstants {

    public static final int MAGIC = 0x4A524550; // "JREP"
    public static final short VERSION = 1;
    public static final int HEADER_BYTES = 16;

    private InterBrokerConstants() {}
}
