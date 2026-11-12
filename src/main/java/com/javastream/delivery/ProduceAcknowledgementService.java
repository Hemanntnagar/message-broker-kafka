package com.javastream.delivery;

import com.javastream.exception.JavaStreamException;
import com.javastream.partition.PartitionId;
import com.javastream.replication.ReplicaManager;
import com.javastream.storage.BrokerMessageStore;

import java.util.Objects;
import java.util.Set;

/** Applies producer acknowledgement levels after a record is appended. */
public final class ProduceAcknowledgementService {

    private final ReplicaManager replicaManager;
    private final int localBrokerId;

    public ProduceAcknowledgementService(ReplicaManager replicaManager, int localBrokerId) {
        this.replicaManager = Objects.requireNonNull(replicaManager, "replicaManager");
        this.localBrokerId = localBrokerId;
    }

    public void awaitAcknowledgement(
            ProducerAckLevel ackLevel, BrokerMessageStore messageStore, PartitionId partitionId) {
        switch (ackLevel) {
            case ACK_NONE -> {
                // leader accepted append in memory
            }
            case ACK_LEADER -> messageStore.flush();
            case ACK_ALL_REPLICAS -> {
                messageStore.flush();
                Set<Integer> inSync = replicaManager.inSyncReplicaIds(partitionId);
                if (inSync.isEmpty() || !inSync.contains(localBrokerId)) {
                    throw new JavaStreamException(
                            "ACK_ALL_REPLICAS cannot be satisfied: leader is not in the ISR for "
                                    + partitionId);
                }
                // Single-broker clusters treat ISR={local} as satisfied; full multi-broker
                // replication will extend this check when replication streams land.
            }
        }
    }
}
