# JavaStream Architecture

## Goals

Deliver a **readable**, **testable** message log with strict layering: networking, protocol, broker orchestration, and storage evolve independently. **Apache Kafka is not embedded** — JavaStream implements its own protocols and persistence.

## System context

```text
┌──────────────────────────────────────────────────────────────────┐
│  JavaStreamApplication → JavaStreamServer → BrokerNode             │
│    • shutdown hooks (SIGINT / SIGTERM)                             │
│    • MonitoringHttpServer (/health, /ready, /metrics, /admin)      │
└────────────────────────────┬─────────────────────────────────────┘
                             │
         ┌───────────────────┴───────────────────┐
         ▼                                       ▼
   TcpBrokerNode                         ClusterBrokerNode
   (cluster.enabled=false)               (cluster + JREP replication)
         │                                       │
         └───────────────────┬───────────────────┘
                             ▼
                    BrokerServer (TCP acceptor)
                             │
                    ClientHandler (per connection)
                             ▼
                    BrokerRequestDispatcher
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
   TopicManager      ConsumerGroupManager   ClusterManager *
   Partition logs     offset catalog        (* cluster only)
         │
         ▼
   FileMessageStore / InMemoryMessageStore
```

## Package map (current)

| Package | Responsibility |
|---------|----------------|
| `server` | Process entry, `BrokerServer`, `ClientHandler` |
| `broker` | `TcpBrokerNode`, `ClusterBrokerNode`, request dispatch |
| `protocol` | JSTR v1 codec, operation codes — [PROTOCOL.md](PROTOCOL.md) |
| `storage` | `FileMessageStore`, segments, indexes — [STORAGE_ENGINE.md](STORAGE_ENGINE.md) |
| `topic` / `partition` | Topic registry, `PartitionId`, leadership metadata |
| `producer` / `consumer` | TCP client libraries + `examples` |
| `group` | Consumer group coordinator |
| `replication` / `cluster` | Inter-broker replication, membership — [REPLICATION.md](REPLICATION.md) |
| `delivery` | Producer ack levels, high watermark helpers |
| `config` | Layered configuration — [CONFIGURATION.md](CONFIGURATION.md) |
| `metrics` / `monitoring` | Micrometer registry, HTTP export |
| `admin` | Admin CLI and JSON admin service |
| `security` | Optional auth, ACL, TLS — [SECURITY.md](SECURITY.md) |
| `benchmark` | JMH and load generator — [BENCHMARKS.md](BENCHMARKS.md) |

## Configuration

Immutable **`JavaStreamConfig`** from **`ConfigLoader`**: classpath defaults → optional file → `JAVASTREAM_*` environment variables.

## Dependency rules

1. **Storage** must not depend on protocol or server packages.
2. **Protocol** does not import storage; requests are handled in the broker layer.
3. **Client packages** (`producer`, `consumer`, `examples`) are not depended on by the broker core.
4. Prefer constructor injection over static service locators.

## Threading model

- Virtual threads per client connection (`ClientHandler`).
- Cluster heartbeat and replication use dedicated broker-side executors.
- Periodic log flush uses a virtual thread when `storage.flush.policy=PERIODIC`.

## Deployment

- **JAR**: fat jar from Maven assembly; main class `com.javastream.server.JavaStreamApplication`.
- **Docker**: see [Dockerfile](Dockerfile) and [docker-compose.yml](docker-compose.yml).
- **CI**: GitHub Actions `verify` on JDK 26.

## Testing strategy

- Unit tests for codec, config, storage indexes, security primitives.
- Integration tests spin real `BrokerServer` / `ClusterBrokerNode` on ephemeral ports.
- See `src/test/java/com/javastream/server/*IntegrationTest.java`.
