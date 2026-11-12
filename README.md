# JavaStream

JavaStream is a **Kafka-inspired** distributed message broker built in Java. This repository is **Phase 0**: project scaffolding, configuration, modular packages, and lifecycle entry points—**no wire protocol or storage implementation yet**.

## Requirements

- Java **26**
- Apache Maven **3.9+** (or use the included `./mvnw` wrapper)

## Build & test

```bash
./mvnw clean verify
```

## Run the broker (Phase 0 stub)

```bash
mvn -q exec:java -Dexec.mainClass=com.javastream.server.JavaStreamApplication
```

Or after packaging:

```bash
mvn -q package
java -jar target/javastream-0.1.0-SNAPSHOT.jar
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
