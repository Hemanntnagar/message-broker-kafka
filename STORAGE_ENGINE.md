# Storage engine

JavaStream persists partition logs on disk when the broker uses **`FileMessageStore`** (default for production `JavaStreamApplication`). An in-memory store exists for tests and embedded benchmarks.

## Layout

Under `storage.data.dir` (default `./data`):

```text
data/
  metadata/
    topic.catalog          # topic names and partition counts
    consumer-offsets.catalog  # group offset commits (when durable)
  topics/
    <topic>/
      <partition>/
        00000000000000000000.log   # segment files
        00000000000000000000.index # sparse offset index
```

`StorageLayout` and `TopicCatalog` manage metadata; each `PartitionId` maps to a `FileMessageLog` with one or more `LogSegment` files.

## Record format

`LogRecordFormat` stores:

- CRC32C over record bytes
- Attributes, message id, timestamp
- Key / value length-prefixed bytes
- Optional headers map

Append returns the logical offset; `SegmentIndex` maps offsets to file positions (stride configured by `storage.index.stride.records`).

## Durability

| `storage.flush.policy` | Behaviour |
|------------------------|-----------|
| `NONE` | OS page cache only between periodic broker events |
| `PERIODIC` | Background virtual thread calls `FileChannel.force` on interval |
| `SYNC` | `force` after each append (highest latency, strongest single-node durability) |

Segment roll occurs when active segment size exceeds `storage.segment.max.bytes`.

## Recovery

On startup, `FileMessageStore` scans topic directories, reopens segments, validates CRC on replay, and restores `nextMessageId`. Corrupt segments fail fast with `LogStorageException` (see storage integration tests).

## In-memory mode

`InMemoryMessageStore` backs unit tests and `EmbeddedBenchmarkBroker`; it does not survive process restart.

## Known limitations

- No compaction or tiered storage
- No log truncation API exposed to operators (retention is not yet configurable via properties)
- Single broker directory per process; no shared filesystem clustering

See also [DELIVERY_GUARANTEES.md](DELIVERY_GUARANTEES.md) and [FAULT_TOLERANCE.md](FAULT_TOLERANCE.md).
