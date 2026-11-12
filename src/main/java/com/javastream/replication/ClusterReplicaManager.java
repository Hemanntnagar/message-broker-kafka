package com.javastream.replication;

import com.javastream.partition.PartitionId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tracks replica log end offsets, ISR membership, and the partition high watermark on the leader.
 */
public final class ClusterReplicaManager implements ReplicaManager {

    private final int localBrokerId;
    private final long replicaLagMaxOffsets;

    private final ConcurrentHashMap<PartitionId, PartitionReplicaState> partitions = new ConcurrentHashMap<>();

    public ClusterReplicaManager(int localBrokerId) {
        this(localBrokerId, 0);
    }

    public ClusterReplicaManager(int localBrokerId, long replicaLagMaxOffsets) {
        this.localBrokerId = localBrokerId;
        this.replicaLagMaxOffsets = replicaLagMaxOffsets;
    }

    @Override
    public Set<Integer> inSyncReplicaIds(PartitionId partition) {
        return state(partition).inSyncReplicaIds();
    }

    @Override
    public void markReplicaInSync(PartitionId partition, int replicaBrokerId) {
        state(partition).markInSync(replicaBrokerId);
    }

    public void configureReplicas(PartitionId partition, int leaderId, java.util.List<Integer> replicaIds) {
        state(partition).configure(leaderId, replicaIds);
    }

    public void recordLeaderAppend(PartitionId partition, long offset) {
        state(partition).recordLeaderOffset(offset);
    }

    public void recordReplicaOffset(PartitionId partition, int brokerId, long logEndOffset) {
        state(partition).recordReplicaOffset(brokerId, logEndOffset);
    }

    public long highWatermark(PartitionId partition) {
        return state(partition).highWatermark();
    }

    public void awaitThrough(PartitionId partition, long offset, long timeoutMs) {
        state(partition).awaitThrough(offset, timeoutMs);
    }

    public void removeBrokerFromIsr(PartitionId partition, int brokerId) {
        state(partition).removeFromIsr(brokerId);
    }

    private PartitionReplicaState state(PartitionId partition) {
        return partitions.computeIfAbsent(partition, ignored -> new PartitionReplicaState(localBrokerId, replicaLagMaxOffsets));
    }

    private static final class PartitionReplicaState {
        private final int localBrokerId;
        private final long replicaLagMaxOffsets;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition caughtUp = lock.newCondition();

        private int leaderId = -1;
        private Set<Integer> assignedReplicas = Set.of();
        private final Map<Integer, Long> replicaLogEnd = new ConcurrentHashMap<>();
        private final Set<Integer> inSync = ConcurrentHashMap.newKeySet();
        private long leaderLogEnd = -1;
        private long highWatermark = 0;

        PartitionReplicaState(int localBrokerId, long replicaLagMaxOffsets) {
            this.localBrokerId = localBrokerId;
            this.replicaLagMaxOffsets = replicaLagMaxOffsets;
        }

        void configure(int leaderId, java.util.List<Integer> replicaIds) {
            lock.lock();
            try {
                this.leaderId = leaderId;
                this.assignedReplicas = Set.copyOf(replicaIds);
                this.inSync.clear();
                for (int id : replicaIds) {
                    replicaLogEnd.putIfAbsent(id, 0L);
                }
                if (replicaIds.contains(localBrokerId)) {
                    inSync.add(localBrokerId);
                }
                recomputeHighWatermark();
            } finally {
                lock.unlock();
            }
        }

        Set<Integer> inSyncReplicaIds() {
            lock.lock();
            try {
                return Set.copyOf(inSync);
            } finally {
                lock.unlock();
            }
        }

        void markInSync(int brokerId) {
            lock.lock();
            try {
                inSync.add(brokerId);
                recomputeHighWatermark();
                caughtUp.signalAll();
            } finally {
                lock.unlock();
            }
        }

        void removeFromIsr(int brokerId) {
            lock.lock();
            try {
                inSync.remove(brokerId);
                recomputeHighWatermark();
            } finally {
                lock.unlock();
            }
        }

        void recordLeaderOffset(long offset) {
            lock.lock();
            try {
                leaderLogEnd = offset;
                replicaLogEnd.put(localBrokerId, offset + 1);
                if (localBrokerId == leaderId) {
                    inSync.add(localBrokerId);
                }
                recomputeHighWatermark();
            } finally {
                lock.unlock();
            }
        }

        void recordReplicaOffset(int brokerId, long logEndOffset) {
            lock.lock();
            try {
                replicaLogEnd.put(brokerId, logEndOffset);
                if (assignedReplicas.contains(brokerId)) {
                    long leaderEnd = replicaLogEnd.getOrDefault(leaderId, leaderLogEnd + 1);
                    if (brokerId == leaderId || logEndOffset >= leaderEnd - replicaLagMaxOffsets) {
                        inSync.add(brokerId);
                    }
                }
                recomputeHighWatermark();
                if (highWatermark > 0) {
                    caughtUp.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        long highWatermark() {
            lock.lock();
            try {
                return highWatermark;
            } finally {
                lock.unlock();
            }
        }

        void awaitThrough(long offset, long timeoutMs) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            lock.lock();
            try {
                while (true) {
                    boolean allCaughtUp = true;
                    int isrFollowers = 0;
                    for (int follower : followerTargets()) {
                        if (!inSync.contains(follower)) {
                            continue;
                        }
                        isrFollowers++;
                        long end = replicaLogEnd.getOrDefault(follower, 0L);
                        if (end <= offset) {
                            allCaughtUp = false;
                            break;
                        }
                    }
                    if (isrFollowers == 0 || allCaughtUp) {
                        recomputeHighWatermark();
                        return;
                    }
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        throw new com.javastream.exception.JavaStreamException(
                                "Timed out waiting for ISR to replicate offset " + offset);
                    }
                    caughtUp.awaitNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(200)));
                    recomputeHighWatermark();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new com.javastream.exception.JavaStreamException("Interrupted waiting for replication", e);
            } finally {
                lock.unlock();
            }
        }

        private Set<Integer> followerTargets() {
            Set<Integer> followers = new HashSet<>(assignedReplicas);
            followers.remove(leaderId);
            return followers;
        }

        private void recomputeHighWatermark() {
            if (assignedReplicas.isEmpty()) {
                highWatermark = Math.max(highWatermark, leaderLogEnd);
                return;
            }
            if (inSync.isEmpty()) {
                return;
            }
            long minEnd = Long.MAX_VALUE;
            for (int replica : inSync) {
                long end = replicaLogEnd.getOrDefault(replica, 0L);
                minEnd = Math.min(minEnd, end);
            }
            if (minEnd == Long.MAX_VALUE) {
                return;
            }
            highWatermark = Math.max(highWatermark, minEnd - 1);
            if (leaderLogEnd >= 0) {
                long target = leaderLogEnd;
                if (inSync.containsAll(assignedReplicas) && minEnd > target) {
                    highWatermark = Math.max(highWatermark, target);
                }
            }
        }
    }
}
