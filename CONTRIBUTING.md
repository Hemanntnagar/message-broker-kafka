# Contributing to JavaStream

Thank you for improving JavaStream. This project is an educational Kafka-inspired broker — clarity and test coverage matter more than feature breadth.

## Development setup

1. Install **JDK 26** and use the Maven wrapper: `./mvnw`.
2. Build and test: `./mvnw clean verify`
3. IDE: import as a Maven project; main class `com.javastream.server.JavaStreamApplication`.

## Code style

- Match existing packages: small classes, constructor injection, no DI framework.
- **Dependency rules** (see [ARCHITECTURE.md](ARCHITECTURE.md)): storage must not import protocol/server; protocol must not import storage directly.
- Prefer immutable config (`JavaStreamConfig`) and explicit error types over unchecked failures.

## Tests

- Unit tests: `src/test/java`
- Integration tests: broker lifecycle, storage, cluster, security — run via Surefire in `verify`
- Add or update tests with behaviour changes; do not mark unfinished features as complete in docs.

## Pull requests

1. Fork / branch from `main` (or `master` if that is the default).
2. Keep commits focused; CI runs `./mvnw clean verify` (see [.github/workflows/ci.yml](.github/workflows/ci.yml)).
3. Update user-facing docs when changing protocol, config keys, or operations.
4. Do not commit secrets (`.env`, keystores, password hashes for real users).

## Docker / release checks

Before a release tag:

```bash
./mvnw clean verify
COMPOSE_PROFILES=single docker compose up -d --build
curl -fsS http://127.0.0.1:8080/health
```

Cluster profile:

```bash
COMPOSE_PROFILES=cluster docker compose up -d --build
```

## Reporting issues

Include Java version, OS, broker config (redacted), steps to reproduce, and relevant log excerpts from `logs/javastream.log`.
