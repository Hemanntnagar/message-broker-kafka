# Configuration reference

JavaStream loads configuration in order (later wins):

1. `src/main/resources/javastream-default.properties` (classpath)
2. Optional file: `JAVASTREAM_CONFIG_FILE` or `-Djavastream.config.file=...`
3. Environment variables: `JAVASTREAM_<KEY>` → dotted property (e.g. `JAVASTREAM_NETWORK_LISTEN_PORT` → `network.listen.port`)

## Core broker

| Property | Env example | Default | Description |
|----------|-------------|---------|-------------|
| `environment` | `JAVASTREAM_ENVIRONMENT` | `development` | Deployment label (logging context) |
| `broker.id` | `JAVASTREAM_BROKER_ID` | `0` | Unique broker id in a cluster |
| `network.listen.host` | `JAVASTREAM_NETWORK_LISTEN_HOST` | `0.0.0.0` | Client protocol bind address |
| `network.listen.port` | `JAVASTREAM_NETWORK_LISTEN_PORT` | `9092` | Client protocol TCP port |
| `storage.data.dir` | `JAVASTREAM_STORAGE_DATA_DIR` | `./data` | Broker data root (topics, metadata) |
| `topic.default.partition.count` | `JAVASTREAM_TOPIC_DEFAULT_PARTITION_COUNT` | `1` | Default when creating topics without explicit count |

## Storage tuning

Read via `ConfigLoader.loadPropertiesForStorage()` (same layering as above).

| Property | Default | Description |
|----------|---------|-------------|
| `storage.segment.max.bytes` | `134217728` | Max segment file size before roll |
| `storage.flush.policy` | `PERIODIC` | `NONE`, `PERIODIC`, or `SYNC` |
| `storage.flush.interval.ms` | `1000` | Period for `PERIODIC` flush |
| `storage.index.stride.records` | `4096` | Sparse index stride per segment |

## Cluster

| Property | Env example | Default | Description |
|----------|-------------|---------|-------------|
| `cluster.enabled` | `JAVASTREAM_CLUSTER_ENABLED` | `false` | Multi-broker replication mode |
| `cluster.coordinator.id` | `JAVASTREAM_CLUSTER_COORDINATOR_ID` | `0` | Broker id of membership coordinator |
| `cluster.coordinator.host` | `JAVASTREAM_CLUSTER_COORDINATOR_HOST` | `127.0.0.1` | Coordinator hostname (Docker service name) |
| `cluster.coordinator.port` | `JAVASTREAM_CLUSTER_COORDINATOR_PORT` | `0` | Coordinator **replication** port |
| `network.replication.port` | `JAVASTREAM_NETWORK_REPLICATION_PORT` | `0` | This broker’s inter-broker port (`0` = ephemeral) |
| `cluster.replication.factor` | `JAVASTREAM_CLUSTER_REPLICATION_FACTOR` | `1` | Target replicas per partition |
| `cluster.session.timeout.ms` | `JAVASTREAM_CLUSTER_SESSION_TIMEOUT_MS` | `15000` | Broker heartbeat expiry |
| `cluster.heartbeat.interval.ms` | `JAVASTREAM_CLUSTER_HEARTBEAT_INTERVAL_MS` | `2000` | Heartbeat cadence |

## Monitoring and admin

| Property | Default | Description |
|----------|---------|-------------|
| `monitoring.http.host` | `127.0.0.1` | Bind address for HTTP (`0.0.0.0` in containers) |
| `monitoring.http.port` | `8080` | Port; `≤ 0` disables HTTP |
| `admin.api.token` | _(empty)_ | Optional bearer token for `/admin/*` |

Endpoints: `/health`, `/ready`, `/metrics` (Prometheus text), `/admin/*`.

## Security

See [SECURITY.md](SECURITY.md). Summary:

| Property | Default |
|----------|---------|
| `security.client.auth.enabled` | `false` |
| `security.acl.enabled` | `false` |
| `security.max.message.bytes` | `1048576` |
| `security.max.connections` | `2048` |
| `network.max.request.bytes` | `1048576` |
| `network.tls.enabled` | `false` |
| `network.tls.keystore.path` | _(empty)_ |

ACL rules and user hashes are loaded from properties or `security.users.file` when auth is enabled.

## Docker

The image entrypoint (`docker/entrypoint.sh`) sets:

- `JAVASTREAM_STORAGE_DATA_DIR=/var/lib/javastream/data`
- `JAVASTREAM_LOG_DIR=/var/lib/javastream/logs` (symlinked to `logs/` for Logback)

Use [docker-compose.yml](docker-compose.yml) profiles `single` or `cluster` — see [README.md](README.md).
