# JavaStream — Phase 13 release report

**Date:** 2026-11-13  
**Version:** `0.1.0-SNAPSHOT`  
**JDK:** 26 (Temurin / Oracle tested locally)

## Executive summary

Phase 13 adds **production-oriented Docker packaging**, **Compose profiles** (single broker and three-broker cluster), **runnable client examples**, consolidated **operator documentation**, and a local **verification script**. Core broker features from phases 1–12 are unchanged; this phase focuses on reproducible deployment and public presentation.

## Implemented features (cumulative)

| Capability | Status |
|------------|--------|
| JSTR v1 TCP protocol (produce, fetch, topics, groups, auth) | Complete |
| Durable segment storage + recovery | Complete |
| Consumer groups (round-robin assignor) | Complete |
| Multi-broker replication + leader failover | Complete (educational model) |
| Producer ack levels + idempotency cache | Complete |
| Monitoring HTTP + Prometheus metrics | Complete |
| Admin JSON API + CLI stub | Complete |
| Optional PBKDF2 auth, ACLs, connection limits | Complete |
| TLS server sockets | Partial — see [SECURITY.md](SECURITY.md) |
| Docker multi-stage image, volumes, health check, SIGTERM | Complete (Phase 13) |
| Docker Compose single + 3-broker cluster | Complete (Phase 13) |
| Client example programs | Complete (Phase 13) |
| Apache Kafka protocol / client compatibility | **Not implemented** |
| KRaft/Raft consensus | **Not implemented** |
| Dynamic ACL admin API | **Not implemented** |
| Log retention policies (operator-facing) | **Not implemented** |

## Phase 13 deliverables

| Item | Location |
|------|----------|
| Dockerfile (multi-stage build) | [Dockerfile](Dockerfile) |
| Entrypoint, log/data volumes | [docker/entrypoint.sh](docker/entrypoint.sh) |
| Compose (profiles `single`, `cluster`) | [docker-compose.yml](docker-compose.yml) |
| Examples | `com.javastream.examples.*` |
| Docs index | [README.md](README.md) |
| Release verification script | [scripts/phase13-verify.sh](scripts/phase13-verify.sh) |

## Test results

Command:

```bash
./mvnw clean verify
```

| Metric | Result (2026-11-13, this workspace) |
|--------|-------------------------------------|
| Surefire test classes | 31 |
| Surefire tests | **107** |
| Failures / errors | **0** |
| SpotBugs (CI) | Skipped by default (`skipSpotbugs=true`) |

Representative integration coverage: `ClusterReplicationIntegrationTest`, `FileMessageStoreIntegrationTest`, `ConsumerGroupIntegrationTest`, `SecurityIntegrationTest`, `MonitoringIntegrationTest`, `FaultToleranceIntegrationTest`.

## Final verification checklist

| Step | Result | Notes |
|------|--------|-------|
| 1. Clone into clean directory | **Not re-run here** | Use `git clone` + `./mvnw verify` on your machine |
| 2. Build from scratch | **Pass** | `./mvnw clean verify` |
| 3. Automated tests | **Pass** | 107 tests, 0 failures |
| 4. Start single broker | **Pass** | JAR + `phase13-verify.sh` |
| 5. Publish and consume | **Pass** | `ProducerExample` / `ConsumerExample` |
| 6. Restart + persistence | **Pass** | Smoke script restarts broker, consumes from disk |
| 7. Multi-broker cluster (JVM) | **Pass** | `ClusterReplicationIntegrationTest` in verify |
| 8. Verify replication | **Pass** | Same test suite |
| 9. Docker Compose | **Manual** | Docker daemon was not available in the agent runtime; image builds on `eclipse-temurin:26-*` when Docker is running |
| 10. Monitoring endpoints | **Pass** | `/health`, `/ready`, `/metrics` in smoke + `MonitoringIntegrationTest` |

## Known bugs and limitations

1. **Cluster consensus** — single coordinator broker; split-brain and coordinator loss are not handled like production Kafka.
2. **Produce to non-leader** — returns `BAD_REQUEST`; clients must discover leaders (limited metadata ergonomics).
3. **Security** — high-level clients do not auto-invoke `CLIENT_AUTH`; replication traffic is plaintext.
4. **TLS** — server TLS is supported when configured; operational hardening gaps documented in [SECURITY.md](SECURITY.md).
5. **Kafka compatibility** — wire format and APIs are JavaStream-specific only.
6. **Benchmarks** — [BENCHMARK_REPORT.md](BENCHMARK_REPORT.md) is machine-specific; re-run before performance claims.
7. **License** — still marked TBD in README.

## Suggested post-release checks (operator)

```bash
COMPOSE_PROFILES=single docker compose --profile single up -d --build
curl -fsS http://127.0.0.1:8080/health
JAVASTREAM_EXAMPLE_BROKER_PORT=9092 ./scripts/run-examples.sh com.javastream.examples.PublishAndFetchExample

COMPOSE_PROFILES=cluster docker compose --profile cluster up -d --build
curl -fsS http://127.0.0.1:8081/health
```

## Documentation map

[README.md](README.md) · [ARCHITECTURE.md](ARCHITECTURE.md) · [PROTOCOL.md](PROTOCOL.md) · [STORAGE_ENGINE.md](STORAGE_ENGINE.md) · [REPLICATION.md](REPLICATION.md) · [CONFIGURATION.md](CONFIGURATION.md) · [BENCHMARKS.md](BENCHMARKS.md) · [CONTRIBUTING.md](CONTRIBUTING.md) · [QA_REPORT.md](QA_REPORT.md)
