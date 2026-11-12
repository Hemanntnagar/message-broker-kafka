# Message delivery guarantees (Phase 7)

JavaStream exposes delivery semantics through **producer acknowledgement levels**, **retries**, **idempotent produce sequence numbers**, and **consumer offset commit timing**. None of these alone provide Kafka-style **exactly-once end-to-end processing**.

## Producer acknowledgement levels

| Level | Behavior |
|-------|----------|
| `ACK_NONE` | Record is accepted on the leader; response is sent without waiting for `flush()`. Fastest; weakest durability if the process crashes before pages are written. |
| `ACK_LEADER` | Record is appended, then the broker applies the configured flush policy before responding. Default for clients. |
| `ACK_ALL_REPLICAS` | Leader flushes and verifies the partition ISR includes the local broker. On a **single broker** this matches `ACK_LEADER`. Full multi-replica safety requires inter-broker replication (not yet implemented). |

Wire encoding: optional 21-byte trailer on `PRODUCE` (see `PROTOCOL.md`).

### Retries and timeouts

`ReliableProducerClient` retries failed requests up to `maxRetries` with `retryBackoff`, using a per-request socket timeout (`requestTimeout`). Retries are safe when **idempotent mode** is enabled (`producerId` + monotonic `sequenceNumber` per topic partition).

### Idempotent producer

The broker keeps `(producerId, topic, partition) → lastSequence, lastAppend`. Retries with the **same** sequence return the original `(messageId, offset)` without a duplicate append. Gaps or regressions in the sequence are rejected.

**Important:** Idempotent produce prevents **duplicate log records** for retried writes. It does **not** make downstream processing exactly-once.

## Consumer delivery

| Mode | How |
|------|-----|
| **Automatic offset commit** | `ConsumerDeliverySettings.autoCommitOffsets = true` — offset committed after successful processing (`ReliableGroupConsumerClient` / `ReliableConsumerClient`). |
| **Manual offset commit** | `autoCommitOffsets = false` — application calls `commitOffsets` / `acknowledge` when ready. |
| **Ack after processing** | Offsets / partition high-water marks move only after `MessageHandler` succeeds (or poison pill is sent to DLQ). |
| **Retryable failures** | Throw `RetryableProcessingException` to retry up to `maxProcessingRetries`. |
| **Dead-letter topic** | After retries are exhausted, the record is published to `deadLetterTopic` with `dlq.*` headers. |

`GroupConsumerClient` still advances in-memory read positions during `poll`; committed offsets define restart positions for at-least-once.

## Delivery semantics

### At-most-once

- Producer: `ACK_NONE`, no retries.
- Consumer: commit/ack **before** processing (not the default in `Reliable*` clients), or ignore failures without retry.

Messages may be **lost** (never processed) but are not duplicated by the client library.

### At-least-once (supported)

- Producer: retries with idempotency (no duplicate records in the log).
- Consumer: commit/ack **after** successful processing; on crash before commit, the message is delivered again.

Duplicates can appear in application processing; handlers should be **idempotent** or deduplicate using `messageId`.

### Exactly-once processing — limitations

JavaStream **does not** provide exactly-once consumption across:

- broker log,
- consumer offset store, and
- external side effects (databases, APIs, files).

Idempotent producer + at-least-once consumer still allows **duplicate processing** if:

1. Processing succeeds but the commit fails.
2. A consumer restarts after commit but before downstream work completes.
3. Multiple consumers read the same partition (misconfiguration).

True exactly-once needs **transactional** coordination (atomic offset commit + side-effect), typically via a transactional producer, idempotent consumer design, and external store participation. That is out of scope for Phase 7.

## Testing

See `MessageDeliveryGuaranteesIntegrationTest` for failure-injection coverage: network interruption, producer retries, duplicate idempotent requests, consumer commit timing, retry exhaustion, dead-letter routing, and offset recovery.
