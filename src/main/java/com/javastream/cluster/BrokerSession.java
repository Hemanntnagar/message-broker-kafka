package com.javastream.cluster;

import com.javastream.partition.PartitionId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Coordinator-side session metadata for one broker. */
final class BrokerSession {

    final BrokerEndpoint endpoint;
    volatile BrokerSessionState state = BrokerSessionState.AVAILABLE;
    volatile long lastHeartbeatMillis;
    volatile long firstMissedHeartbeatMillis;
    int consecutiveFailedProbes;
    final ConcurrentHashMap<PartitionId, Long> reportedLogEnds = new ConcurrentHashMap<>();

    BrokerSession(BrokerEndpoint endpoint, long nowMillis) {
        this.endpoint = endpoint;
        this.lastHeartbeatMillis = nowMillis;
    }

    BrokerSession withEndpoint(BrokerEndpoint endpoint) {
        BrokerSession copy = new BrokerSession(endpoint, lastHeartbeatMillis);
        copy.state = state;
        copy.firstMissedHeartbeatMillis = firstMissedHeartbeatMillis;
        copy.consecutiveFailedProbes = consecutiveFailedProbes;
        copy.reportedLogEnds.putAll(reportedLogEnds);
        return copy;
    }

    Map<PartitionId, Long> reportedLogEndsSnapshot() {
        return Map.copyOf(reportedLogEnds);
    }
}
