# JavaStream wire protocol (v1)

Custom binary framing for client–broker TCP connections. This is **not** the Apache Kafka protocol.

Runnable clients: see [README.md](README.md#client-examples). Low-level framing tests live in `com.javastream.protocol.ProtocolEncoderDecoderTest`.

All multi-byte fields are **big-endian**.

## Frame layout

### Request

| Offset | Size | Field |
|--------|------|--------|
| 0 | 4 | Magic `0x4A535452` (`"JSTR"`) |
| 4 | 2 | Protocol version (`1`) |
| 6 | 4 | Correlation ID |
| 10 | 2 | Operation code |
| 12 | 4 | Payload length (≥ 0) |
| 16 | *N* | Payload |

### Response

| Offset | Size | Field |
|--------|------|--------|
| 0 | 4 | Magic `0x4A535452` |
| 4 | 2 | Protocol version (`1`) |
| 6 | 4 | Correlation ID (matches request) |
| 10 | 2 | Status code |
| 12 | 4 | Payload length (≥ 0) |
| 16 | *N* | Payload |

Header size for both directions: **16 bytes**.

## Operation codes (v1)

| Code | Operation |
|------|-----------|
| 1 | `PING` |
| 2 | `CREATE_TOPIC` |
| 3 | `LIST_TOPICS` |
| 4 | `PRODUCE` |
| 5 | `FETCH` |
| 6 | `DELETE_TOPIC` |
| 7 | `GET_METADATA` |
| 8 | `CONSUMER_ACK` |
| 9 | `JOIN_GROUP` |
| 10 | `HEARTBEAT` |
| 11 | `LEAVE_GROUP` |
| 12 | `COMMIT_OFFSETS` |
| 13 | `FETCH_COMMITTED_OFFSETS` |
| 18 | `CLIENT_AUTH` |

## Operation payloads (v1)

### `CREATE_TOPIC` (2)

| Field | Type |
|-------|------|
| Topic name | `uint16` length + UTF-8 |
| Partition count | `int32` (> 0) |

### `PRODUCE` (4)

| Field | Type |
|-------|------|
| Topic name | `uint16` length + UTF-8 |
| Partition hint | `int32` (`-1` = broker assigns) |
| Key | `int32` length (`-1` = null) + bytes |
| Value | `int32` length (`-1` = null) + bytes |
| Headers | `uint16` count, then repeated (`uint16` key len + key + `uint16` value len + value) |

**Delivery extension** (21 bytes, optional — omitted by legacy clients):

| Field | Type |
|-------|------|
| Ack level | `int8` (`0` = none, `1` = leader, `2` = all ISR) |
| Request timeout | `int32` millis (advisory) |
| Producer ID | `int64` (`-1` = not idempotent) |
| Sequence | `int64` (per producer, topic, partition; starts at `0`) |

**Batch extension** (optional — more records in one `PRODUCE` round trip):

| Field | Type |
|-------|------|
| Additional record count | `int32` (≥ 0) |
| Additional records | repeated key, value, headers (same encoding as the first record) |

**OK response payload:** single record — `uint64` messageId, `int32` partition, `int64` offset (20 bytes). Batch — `int32` count, then count × (messageId, partition, offset).

### `FETCH` (5)

| Field | Type |
|-------|------|
| Topic name | `uint16` length + UTF-8 |
| Partition | `int32` |
| Offset | `int64` |
| Max records | `int32` |
| Timeout millis | `int32` (advisory; broker returns immediately) |

**OK response payload:** `int32` record count, then for each record: messageId, partition, offset, timestamp (epoch ms), key, value, headers (same encoding as produce).

### `CONSUMER_ACK` (8)

| Field | Type |
|-------|------|
| Topic name | `uint16` length + UTF-8 |
| Partition | `int32` |
| Offset | `int64` |

### `JOIN_GROUP` (9)

| Field | Type |
|-------|------|
| Group ID | `uint16` length + UTF-8 |
| Member ID | `uint16` length + UTF-8 |
| Topic name | `uint16` length + UTF-8 |

**OK response:** `int32` generation, `int32` assigned count + assigned partitions, `int32` revoked count + revoked partitions, `int32` committed count + repeated (`int32` partition, `int64` offset).

### `CLIENT_AUTH` (18)

| Field | Type |
|-------|------|
| Username | `uint16` length + UTF-8 |
| Password | `uint16` length + UTF-8 (never logged server-side) |

**OK response payload:** `uint8` authenticated (`1` = success), `uint16` message length + UTF-8 advisory text (empty on success). Failed authentication returns status `FORBIDDEN` with the same payload shape (`authenticated = 0`).

### `HEARTBEAT` (10) / `LEAVE_GROUP` (11)

| Field | Type |
|-------|------|
| Group ID | `uint16` length + UTF-8 |
| Member ID | `uint16` length + UTF-8 |

`LEAVE_GROUP` uses the same payload as `HEARTBEAT`. **OK response:** empty.

### `COMMIT_OFFSETS` (12)

| Field | Type |
|-------|------|
| Group ID | `uint16` length + UTF-8 |
| Topic name | `uint16` length + UTF-8 |
| Offset count | `int32` |
| Offsets | repeated `int32` partition + `int64` offset |

**OK response:** empty.

### `FETCH_COMMITTED_OFFSETS` (13)

| Field | Type |
|-------|------|
| Group ID | `uint16` length + UTF-8 |
| Topic name | `uint16` length + UTF-8 |
| Partition count | `int32` (+ partition indices; empty list = all partitions) |

**OK response:** `int32` count + repeated (`int32` partition, `int64` offset). Missing commits use offset `-1`.

## Status codes (v1)

| Code | Status |
|------|--------|
| 0 | `OK` |
| 1 | `BAD_REQUEST` |
| 2 | `NOT_FOUND` |
| 3 | `UNSUPPORTED_VERSION` |
| 4 | `PAYLOAD_TOO_LARGE` |
| 5 | `INTERNAL_ERROR` |
| 6 | `UNKNOWN_OPERATION` |
| 7 | `MALFORMED` |

## Streaming reads

TCP delivers a byte stream. A single `read` may contain zero, part of, or several frames. Use `ProtocolDecoder` to buffer and emit complete requests. Reject unknown protocol versions, invalid magic, negative lengths, and payloads larger than the configured ceiling (default **1 MiB** on the broker).

## Versioning

Only version **1** is defined in this document. Future versions may add fields or new operation codes while keeping the magic constant for demuxing.
