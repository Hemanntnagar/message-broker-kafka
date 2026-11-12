# JavaStream Architecture

## Goals

Build a multi-broker-capable message log with clear boundaries: **networking**, **protocol**, **broker orchestration**, and **storage** stay separate. Components are **interface-first** and wired through **constructor injection**—no DI framework in Phase 0.

Apache Kafka is **not** used as an internal dependency; JavaStream implements its own protocol and storage over time.

## Layering

```text
┌─────────────────────────────────────────────────────────┐
│  server (JavaStreamServer, JavaStreamApplication)       │
│  Process lifecycle, shutdown hooks, config bootstrap      │
└───────────────────────────┬─────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────┐
│  broker (BrokerNode)                                    │
│  Orchestrates topic/partition/replication on this node    │
└─┬──────────────┬──────────────┬──────────────┬──────────┘
  │              │              │              │
  ▼              ▼              ▼              ▼
topic/      partition/     group/        replication/
partition   leadership     coordinator   ReplicaManager
  │              │              │              │
  └──────────────┴──────┬───────┴──────────────┘
                        ▼
                   storage/
                   MessageStore (durable log — later phase)
                        ▲
                        │ (no direct coupling from protocol)
┌───────────────────────┴─────────────────────────────────┐
│  protocol (RequestHandler, ProtocolCodec)                 │
│  Wire framing only; delegates to broker services          │
└───────────────────────────┬─────────────────────────────┘
                            │
                     server.BrokerServer + ClientHandler (Phase 1 TCP)
```

## Module map

| Package | Responsibility | Current state |
|---------|----------------|---------------|
| `server` | JVM entry, TCP acceptor | `JavaStreamServer`, `BrokerServer`, `ClientHandler` |
| `config` | Layered config, env overrides | `ConfigLoader`, `JavaStreamConfig` |
| `broker` | Node coordinator | `TcpBrokerNode` (default), `NoOpBrokerNode` (tests) |
| `protocol` | Encode/decode, dispatch | `ProtocolEncoder`, `ProtocolDecoder`, v1 frames — see `PROTOCOL.md` |
| `topic` / `partition` | Naming & shard identity | Value types + registry interface |
| `storage` | Append/read log | Interface only |
| `producer` / `consumer` | Client APIs | Interfaces only |
| `group` | Consumer groups | Interface only |
| `replication` | ISR / replicas | Interface only |
| `cluster` | Membership, `BrokerId` | Interface only |
| `metrics` | Observability hooks | Interface only |
| `admin` | Operator API / CLI stub | `AdminCliApplication` |
| `common` / `exception` | Shared utilities | Preconditions, base errors |

## Multi-broker design (forward-looking)

- Each process has a **`broker.id`** and optional **`cluster.enabled`**.
- **`PartitionLeader`** and **`ClusterMembership`** will elect/serve leaders per partition across brokers.
- **`ReplicaManager`** tracks in-sync replicas; storage remains local per broker with replication streams added later.

## Configuration model

Immutable **`JavaStreamConfig`** built by **`ConfigLoader`**:

1. `javastream-default.properties` on the classpath  
2. Optional external properties file  
3. `JAVASTREAM_*` environment variables via **`EnvironmentConfigSource`**

Invalid or missing required resources fail fast with **`ConfigurationException`**.

## Dependency rules

1. **Storage must not import protocol or server.**
2. **Protocol must not import storage directly** (goes through broker services).
3. **Producer/consumer packages** are client-oriented; broker internals do not depend on them.
4. Prefer **small classes** over god objects; Phase 0 uses no-op implementations where behavior is not yet defined.

## Testing strategy (Phase 0)

JUnit 5 unit tests for configuration loading, environment key mapping, server lifecycle, and shared validators. Integration and protocol tests arrive with networking and storage.

## Phase 1 networking

- **`BrokerServer`**: `ServerSocket` accept loop on a virtual thread; binds `JavaStreamConfig.listenHost()` / `listenPort()` (default **9092**).
- **`ClientHandler`**: one virtual thread per connection; JavaStream v1 binary frames via `ProtocolDecoder` / `ProtocolEncoder`; socket read timeout **30s**; max payload **1 MiB**; `PING` and stub `OK` responses until broker services land.

## Phase 2 protocol

- **`PROTOCOL.md`**: magic, version, correlation ID, operation/status, length-prefixed payload.
- **`ProtocolDecoder`**: streaming reassembly (partial reads, multiple frames per read); rejects bad magic, version, and oversize lengths.
- **`ProtocolEncoder`**: symmetric request/response framing.
- Graceful shutdown closes the listening socket and all tracked client sockets.

## Next phases (not in scope here)

1. Request pipeline (`protocol` + broker services)
2. Segment-based `MessageStore`
3. Topic creation and partition assignment on `BrokerNode`
4. Replication and cluster coordination
5. Producer/consumer client libraries
