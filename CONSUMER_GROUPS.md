# Consumer groups vs independent consumers

JavaStream supports two ways to read from topics. They differ in who chooses partitions, how progress is tracked, and what happens when consumers come and go.

## Independent consumers (`ConsumerClient`)

An **independent consumer** connects to the broker, subscribes to a topic name, and **chooses a partition** (default `0`) before each `FETCH`. The client keeps the next offset to read in local memory (`nextOffsets` per topic, not per partition unless you call `setPartition`).

- **Partition choice**: entirely on the client (`setPartition`).
- **Progress**: optional `CONSUMER_ACK` updates a broker-side high-water mark per **partition only** (not scoped to a consumer identity). Multiple unrelated clients can read the same partition from different offsets independently.
- **Scaling**: adding consumers does **not** coordinate work; two consumers on partition `0` will both read from their own cursor unless you coordinate externally.
- **Use when**: single partition, tooling, or you manage assignment yourself.

## Consumer groups (`GroupConsumerClient` + `ConsumerGroupManager`)

A **consumer group** is a named set of members (`groupId`) that jointly consumes **one subscribed topic**. The broker **assigns partitions** among active members (round-robin in Phase 6), maintains **membership and heartbeats**, and **rebalances** when members join, leave, or expire.

- **Partition choice**: broker assignment after `JOIN_GROUP`; each partition is owned by at most one live member in the group.
- **Progress**: `COMMIT_OFFSETS` stores offsets per **`(groupId, topic, partition)`**, independent of other groups on the same topic. Commits are persisted in `metadata/consumer-offsets.catalog` when the broker uses durable storage.
- **Scaling**: add members to spread partitions; remove or failed members trigger reassignment (revoked partitions stop being polled; new assignments resume from committed offsets).
- **Use when**: horizontal scale-out, at-least-once processing with shared progress, Kafka-style group semantics.

## Wire operations (v1 extensions)

| Code | Operation | Purpose |
|------|-----------|---------|
| 9 | `JOIN_GROUP` | Register member, receive assignment / revocation |
| 10 | `HEARTBEAT` | Keep session alive |
| 11 | `LEAVE_GROUP` | Graceful exit and rebalance |
| 12 | `COMMIT_OFFSETS` | Persist group offsets |
| 13 | `FETCH_COMMITTED_OFFSETS` | Read committed positions |

See [PROTOCOL.md](PROTOCOL.md) for payload layouts.

## Rebalancing guarantees (Phase 6)

- No two active members in the same group own the same partition.
- Committed offsets are **not** deleted on rebalance; only ownership changes.
- Join responses include **revoked** and **assigned** partition lists so clients can drop cursors for revoked shards and seed new ones from committed positions.
- Group membership updates run under a single coordinator lock per broker process.
