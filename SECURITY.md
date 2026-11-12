# JavaStream security (Phase 12)

JavaStream supports **optional** client authentication, **topic-level ACLs**, request size limits, connection caps, and **optional TLS** on the client TCP listener. All features are disabled by default for backward-compatible local development.

## Client authentication

Enable with:

```properties
security.client.auth.enabled=true
```

Define users with PBKDF2 password hashes (never store plain text in production):

```properties
security.user.alice.password.hash=pbkdf2_sha256$...
```

Or point at a line-oriented file (`username=hash`):

```properties
security.users.file=/etc/javastream/users.cnf
```

Clients authenticate with operation **`CLIENT_AUTH` (18)** before any other operation (except `PING`). See [PROTOCOL.md](PROTOCOL.md).

Generate hashes in a REPL or unit test via `PasswordHasher.hashPassword(char[])`.

## Topic authorization (ACL)

```properties
security.acl.enabled=true
security.acl.rules=alice:orders:produce,consume;bob:logs*:consume
```

Rules are `principal:topicPattern:permission[,permission...]` separated by `;`. Topic patterns support trailing `*` prefix match. Permissions: `PRODUCE`, `CONSUME`, `ADMIN` (topic create/delete when no admin API token is presented).

## Message and request limits

| Property | Default | Purpose |
|----------|---------|---------|
| `security.max.message.bytes` | 1048576 | Max combined key+value+headers per produce batch |
| `network.max.request.bytes` | 1048576 | Max TCP frame payload (protocol decoder) |
| `security.max.connections` | 2048 | Concurrent client connections per broker |

## TLS

JavaStream uses a plain `ServerSocket` by default. To terminate TLS on the broker:

```properties
network.tls.enabled=true
network.tls.keystore.path=/path/to/keystore.p12
network.tls.keystore.password=changeit
network.tls.keystore.type=PKCS12
```

**Limitations (v1):**

- Server-side TLS only (no mutual TLS / client certificates).
- TLS applies to the **client** listener, not inter-broker replication sockets (those remain plain TCP).
- Clients must use `SSLSocket` or a TLS-terminating proxy; the bundled `TcpProtocolConnection` is plain TCP.

## Logging

Credentials from `CLIENT_AUTH` are never written to logs. Request logging records operation, status, and optional authenticated principal only.

## Admin API token

Topic create/delete may still require `admin.api.token` (HTTP monitoring and optional wire extension on create/delete payloads). ACL `ADMIN` permission is an alternative when ACLs are enabled.
