# JavaStream wire protocol (v1)

Custom binary framing for client–broker TCP connections. This is **not** the Apache Kafka protocol.

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
