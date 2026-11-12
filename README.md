# JavaStream

JavaStream is a **Kafka-inspired** distributed message broker built in Java.

**Phase 3** adds synchronous **producer** and **consumer** TCP clients, in-memory topic storage, and broker handling for `CREATE_TOPIC`, `PRODUCE`, `FETCH`, and `CONSUMER_ACK` on top of the Phase 2 wire protocol (default port **9092**).

## Requirements

- Java **26**
- Apache Maven **3.9+** (or use the included `./mvnw` wrapper)

## Build & test

```bash
./mvnw clean verify
```

## Run the broker

Start (listens on `network.listen.host` / `network.listen.port`, default `0.0.0.0:9092`):

```bash
./mvnw -q exec:java -Dexec.mainClass=com.javastream.server.JavaStreamApplication
```

Or after packaging:

```bash
./mvnw -q package
java -jar target/javastream-0.1.0-SNAPSHOT.jar
```

Stop gracefully with **Ctrl+C** (SIGINT). The JVM shutdown hook calls `JavaStreamServer.stop()`, which closes the `ServerSocket` and active client connections.

### Manual TCP smoke test

Wire format is documented in [PROTOCOL.md](PROTOCOL.md) (magic `JSTR`, version **1**, 16-byte header, payload). A minimal `PING` uses operation code **1** and an empty payload; the server responds with status **OK** and the same correlation ID.

For scripted clients, use `ProtocolEncoder` / `ProtocolDecoder` in `com.javastream.protocol` (see protocol tests).

### Producer / consumer example (Phase 3)

```bash
./mvnw -q exec:java -Dexec.mainClass=com.javastream.examples.ProducerConsumerExample
```

High-level clients: `com.javastream.producer.ProducerClient` and `com.javastream.consumer.ConsumerClient`.

Override bind address/port:

```bash
export JAVASTREAM_NETWORK_LISTEN_HOST=127.0.0.1
export JAVASTREAM_NETWORK_LISTEN_PORT=19092
./mvnw -q exec:java -Dexec.mainClass=com.javastream.server.JavaStreamApplication
```

## Configuration

Defaults live in `src/main/resources/javastream-default.properties`.

Override order (later wins):

1. Classpath defaults
2. Optional file (`JAVASTREAM_CONFIG_FILE` or `-Djavastream.config.file=...`)
3. Environment variables prefixed with `JAVASTREAM_` (e.g. `JAVASTREAM_NETWORK_LISTEN_PORT=9093` → `network.listen.port`)

## Package layout

`com.javastream` — `server`, `protocol`, `broker`, `producer`, `consumer`, `topic`, `partition`, `storage`, `group`, `replication`, `cluster`, `config`, `metrics`, `admin`, `common`, `exception`

See [ARCHITECTURE.md](ARCHITECTURE.md) for layering and design rules.

## Entry points

| Class | Role |
|-------|------|
| `com.javastream.server.JavaStreamApplication` | Broker process |
| `com.javastream.admin.AdminCliApplication` | Admin CLI stub |

## Logging

SLF4J with Logback: `src/main/resources/logback.xml` (console + rolling file under `logs/`).

## License

TBD.
