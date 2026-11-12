# Phase 8: Multi-broker cluster and replication

JavaStream can run as a **multi-process cluster** with a dedicated **inter-broker TCP protocol** (`JREP` framing on `network.replication.port`). Client traffic continues to use the v1 `JSTR` protocol on `network.listen.port`. Single-broker mode is unchanged when `cluster.enabled=false`.

## Components

| Component | Role |
|-----------|------|
| `ClusterBrokerNode` | Starts client listener, inter-broker listener, and cluster session |
| `ClusterManager` | Coordinator-side registration, heartbeats, replica placement, leader failover |
| `ClusterReplicaManager` | ISR, per-replica log ends, high watermark on the leader |
| `ReplicationEngine` | Leader push replication, follower append, catch-up fetch |
| `InterBrokerServer` / `InterBrokerClient` | Cross-process replication RPCs |

The **cluster coordinator** is the broker with `broker.id == cluster.coordinator.id` (default **0**). It is not a separate process.

## Consistency model (educational — not production consensus)

### Leader election

- **Initial assignment:** For partition `p`, replicas are chosen round-robin across live brokers with replication factor `RF = min(cluster.replication.factor, |brokers|)`. The **leader** is the **smallest broker id** in that replica set.
- **Failover:** When a broker misses heartbeats for `cluster.session.timeout.ms`, the coordinator removes it from membership and, for each affected partition, elects the **smallest alive broker id** still in the replica list.
- **No Raft/ZAB:** This is a **single coordinator** with deterministic rules. Split-brain and unsafe leadership during coordinator partition are **not** prevented.

### In-sync replica (ISR) policy

- A replica is in the ISR if it is assigned to the partition and its reported log end is within `replicaLagMaxOffsets` (0 = fully caught up) of the leader, or it has acknowledged a recent replicate.
- If inter-broker replicate RPCs fail or heartbeats expire, the follower is **removed from the ISR** until it catches up again.

### Commit rules and high watermark

- The leader appends locally, then **pushes** each record to followers over TCP.
- **High watermark** on a partition is derived from the minimum log end offset across **current ISR** members (leader included).
- Records at offsets **above** the high watermark may be lost if the leader fails before ISR replication completes.

### Producer acknowledgement behaviour

| Level | Cluster behaviour |
|-------|-------------------|
| `ACK_NONE` / `ACK_LEADER` | Same as single broker (leader append; optional flush) |
| `ACK_ALL_REPLICAS` | Leader flush + wait until all **assigned followers** report log end **> offset** (best-effort; ISR may shrink on failure) |

Produces to a **non-leader** broker return `BAD_REQUEST` (clients should target the partition leader).

### Data loss boundaries

- **Coordinator broker down:** No new registrations or assignment updates until it returns.
- **Leader dies before replicate completes:** Uncommitted records may be lost; failover may elect a new leader from ISR/lag state.
- **Multiple failures beyond `RF`:** Partitions may become unavailable or lose durability guarantees.

### Replica recovery

1. Follower restarts and **re-registers** with the coordinator.
2. Coordinator pushes current **assignment** (leader + replicas).
3. Follower runs **replica fetch** from the leader from its local log end until caught up.
4. Follower re-enters ISR when offsets match policy.

## Configuration

See `javastream-default.properties`:

- `cluster.enabled=true`
- `cluster.coordinator.id=0`
- `cluster.coordinator.host` / `cluster.coordinator.port` (replication port of broker 0 for other brokers)
- `cluster.replication.factor`
- `cluster.session.timeout.ms` / `cluster.heartbeat.interval.ms`
- `network.replication.port` (`0` = ephemeral)

## Testing

`ClusterReplicationIntegrationTest` exercises three brokers, leader produce, follower replication, follower stop/restart catch-up, and partition leader failover.
