package com.javastream.cluster;

/**
 * Coordinator view of a broker's reachability.
 *
 * <p>A missed heartbeat deadline alone moves a broker to {@link #SUSPECTED_UNAVAILABLE} — that is
 * not treated as permanent failure. Expulsion requires confirmation ({@link #CONFIRMED_UNAVAILABLE}).
 */
public enum BrokerSessionState {
    /** Heartbeats (or replication probes) are within policy. */
    AVAILABLE,
    /** Past the session timeout without a heartbeat — may be process crash, partition, or slow GC. */
    SUSPECTED_UNAVAILABLE,
    /** Probe failure or extended loss of heartbeats — safe to treat as down for failover. */
    CONFIRMED_UNAVAILABLE
}
