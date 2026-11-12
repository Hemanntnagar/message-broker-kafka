# JavaStream Architecture (Phase 0)

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
                     (future network I/O)
```

## Module map

| Package | Responsibility | Phase 0 state |
|---------|----------------|---------------|
| `server` | JVM entry, `ServerLifecycle` | `JavaStreamServer` + main |
| `config` | Layered config, env overrides | `ConfigLoader`, `JavaStreamConfig` |
| `broker` | Node coordinator | `BrokerNode` + `NoOpBrokerNode` |
| `protocol` | Encode/decode, dispatch | Interfaces only |
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

## Next phases (not in scope here)

1. TCP acceptor and request pipeline (`protocol` + `server`)
2. Segment-based `MessageStore`
3. Topic creation and partition assignment on `BrokerNode`
4. Replication and cluster coordination
5. Producer/consumer client libraries
