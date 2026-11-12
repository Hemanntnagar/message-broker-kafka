# JavaStream

JavaStream is a **Kafka-inspired** distributed message broker built in Java.

**Phase 1** adds a TCP listener with virtual-threaded client handlers (default port **9092**). Message storage and produce/consume semantics are not implemented yet.

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

Wire format (Phase 1): **4-byte big-endian length** + payload. The server replies with `0` (4-byte length of an empty body).

```bash
# Example with netcat (send length 5 + "hello")
printf '\x00\x00\x00\x05hello' | nc 127.0.0.1 9092 | xxd
# Expect four zero bytes (empty response payload length)
```

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
