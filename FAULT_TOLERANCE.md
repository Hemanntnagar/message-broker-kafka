# Phase 9: Fault tolerance and recovery

Cluster behaviour under broker loss, replication lag, storage errors, and restarts.

## Failure classes

| Class | Symptoms | Coordinator action | Client impact |
|-------|----------|-------------------|---------------|
| **Process failure** | Heartbeats stop; replication TCP fails | `SUSPECTED_UNAVAILABLE` → probe → `CONFIRMED_UNAVAILABLE` → failover | Retry produce/fetch to new leader |
| **Network partition** | Heartbeats stop but process may still run | Same suspicion path; **timeout alone is not permanent death** | Risk of brief dual visibility until assignment push propagates |
| **Temporary unavailability** | Missed heartbeats then recovery | Returns to `AVAILABLE` on next heartbeat | Transient errors; no membership drop |
| **Disk failure** | Append throws `DiskFullException` or `LogStorageException` (corrupt segment) | Broker may stop accepting writes locally; heartbeats may lapse | Produce fails fast; unclean segments truncated on recovery |

A broker is **removed from membership** only when it is `CONFIRMED_UNAVAILABLE` (failed replication probes and/or heartbeat loss for `2 × cluster.session.timeout.ms`), or when it **deregisters** gracefully.

## Broker liveness

1. Members send **heartbeats** every `cluster.heartbeat.interval.ms` including **per-partition log end offsets** for safe leader election.
2. Past `cluster.session.timeout.ms` without a heartbeat → **`SUSPECTED_UNAVAILABLE`** (still registered).
3. Coordinator runs **TCP connect probes** to the replication port.
4. **Two failed probes** or extended loss → **`CONFIRMED_UNAVAILABLE`** → deregister and **leader failover**.

## Leader epochs (fencing)

Each partition assignment carries a monotonic **`leaderEpoch`**. On leader change the coordinator increments the epoch and pushes assignments.

- The active leader must have `epochRegistry.epoch(partition) >= assignment.leaderEpoch()` before **produce** or **replicate**.
- Followers **reject replicate RPCs** with a stale epoch.
- Prevents a superseded leader from advancing the log after failover **once assignments are current** (see network partition note above).

Failover picks the smallest **alive** replica whose reported log end is **caught up** with the best known end among replicas; otherwise falls back to deterministic smallest alive id.

## Replication and ISR

- Failed replicate RPCs **shrink the ISR** immediately (`ClusterReplicaManager`).
- Followers **catch up** on start and after assignment pushes (`catchUpLocalFollower`).
- `ACK_ALL_REPLICAS` waits for ISR followers through the high watermark policy (Phase 8).

## Consumer groups

- Committed offsets are loaded from the on-disk catalog at broker start (`GroupOffsetStore`).
- Stale members expire via consumer heartbeats; **rebalance** assigns partitions to survivors.
- After broker restart, consumers **re-join** to restore assignment.

## Interrupted writes

- **Idempotent producers** (`producerId` + sequence) deduplicate after reconnect.
- Unclean leader failure may drop records above the last replicated high watermark (see `DELIVERY_GUARANTEES.md`).

## Graceful shutdown

1. Flush the message store.
2. Stop client and inter-broker listeners.
3. Send **`BROKER_DEREGISTER`** to the coordinator (non-coordinator brokers).
4. Coordinator fails over partitions and pushes new assignments without waiting for session expiry.

## Storage recovery

- **Corrupt tail records** in a segment are truncated on open (`LogSegment` scan).
- **CRC mismatch** stops recovery before applying bad bytes (see storage integration tests).
- **Disk full** surfaces as `DiskFullException` on append.

## Testing

`FaultToleranceIntegrationTest` and `BrokerFailureDetectionTest` simulate broker stop, session suspicion, leader epoch bump, follower catch-up, and interrupted produce with idempotency. Run:

```bash
./mvnw test
```
