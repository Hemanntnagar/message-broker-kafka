# JavaStream Phase 12 — Quality Assurance Report

**Date:** 2026-11-12  
**Build:** `./mvnw clean verify` (Java 26)

## Summary

Phase 12 adds a security plane (optional authentication, ACLs, size/connection limits, TLS hook), expands automated tests, enables GitHub Actions CI, and documents operational security constraints.

## Security checklist

| Requirement | Status | Notes |
|-------------|--------|-------|
| Optional client authentication | Done | `security.client.auth.enabled`, `CLIENT_AUTH` op |
| Username / password | Done | Wire payload + `ClientAuthenticator` |
| Secure password hashes | Done | PBKDF2-HMAC-SHA256 (`PasswordHasher`) |
| Topic-level authorization | Done | `TopicAccessControl` + config rules |
| Producer / consumer permissions | Done | `PRODUCE` / `CONSUME` / `ADMIN` |
| Request validation | Done | Existing protocol decoder + dispatcher guards |
| Max message sizes | Done | `MessageSizeValidator`, `PAYLOAD_TOO_LARGE` |
| Resource exhaustion mitigation | Done | Max frame bytes, max connections |
| Malformed protocol protection | Done | `ProtocolDecoder` bounds (existing) |
| Credentials not in logs | Done | No password logging; principal in MDC only |
| TLS | Partial | `SSLServerSocket` when configured; limitations in [SECURITY.md](SECURITY.md) |

## Test coverage added

| Category | Tests |
|----------|-------|
| Unit | `PasswordHasherTest`, `TopicAccessControlTest`, `SecurityConfigLoaderTest` |
| Integration | `SecurityIntegrationTest` |
| Protocol compatibility | `ProtocolCompatibilityTest` |
| Concurrent stress | `ConcurrentClientStressTest` |
| Persistence / replication / fault | Existing suites retained (`FileMessageStoreIntegrationTest`, `ClusterReplicationIntegrationTest`, `FaultToleranceIntegrationTest`, …) |

## Quality review

- **Dead code:** No broad removal pass; security types are fully wired.
- **Concurrency:** Connection set remains `ConcurrentHashMap`; ACL store immutable after load.
- **Resource leaks:** Client handlers close sockets in `finally`; broker stop closes acceptor.
- **Exception handling:** Dispatcher maps decode/domain errors to wire status codes.
- **Configuration:** Invalid security combinations fail fast in `SecurityConfigLoader`.
- **Storage recovery:** Unchanged; covered by existing storage integration tests.

## CI

Workflow [`.github/workflows/ci.yml`](.github/workflows/ci.yml):

- JDK 26 (Temurin)
- `mvn clean verify`
- Surefire report artifacts
- Packaged JAR artifacts (application + fat jar when assembly profile runs in verify)

## Known gaps / follow-ups

1. High-level clients do not auto-authenticate; callers must invoke `TcpProtocolConnection.authenticate`.
2. Replication and inter-broker traffic are not encrypted.
3. ACL management is config-file based (no dynamic API).
4. SpotBugs is not enforced in CI (can be added once baseline is clean).

## Verification command

```bash
./mvnw clean verify
```

Expected: **BUILD SUCCESS**, all unit and integration tests pass.
