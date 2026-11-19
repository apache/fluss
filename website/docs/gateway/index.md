---
sidebar_position: 1
title: Fluss Gateway
---

# Fluss Gateway

:::caution Preview

Fluss Gateway is introduced as a preview in Fluss 1.0. Its API and
configuration may change in later releases.

:::

Fluss Gateway is a stateless REST service for metadata, DDL, and schema-aware
batch writes. Any Gateway instance can handle any request, so instances can be
scaled behind a load balancer.

For an end-to-end walkthrough — ingesting events via the Gateway and querying
 them through a Paimon lakehouse — see 
[Ingest HTTP Events into a Real-Time Lakehouse](../quickstart/gateway-lakehouse.md).
To run the Gateway as a binary distribution or container, see
[Deploying Fluss Gateway](../install-deploy/deploying-gateway.md).

## Capabilities and limitations

| Area | Operations |
| --- | --- |
| Service | Health, readiness, cluster discovery, and OpenAPI 3.1 |
| Metadata | List databases, tables, and partitions; describe tables |
| Database DDL | Create and drop databases |
| Table DDL | Create, validate, alter, and drop tables |
| Partition DDL | Add and drop partitions |
| Records | Batch append, upsert, partial update, and delete |

End-user identity propagation, primary-key or prefix lookup, log scans, and other
record reads are not supported yet.

## Before you start

### Configuration

Configure `gateway.cluster.<id>.bootstrap.servers` for each Fluss cluster. The
examples below use a cluster named `default` and the REST listener at
`127.0.0.1:8080`.

Environment variables override file settings by uppercasing the key and using
double underscores between segments. For example,
`gateway.cluster.default.bootstrap.servers` maps to
`FLUSS_GATEWAY__CLUSTER__DEFAULT__BOOTSTRAP__SERVERS`.

See [`conf/gateway.yaml`](https://github.com/apache/fluss/blob/main/fluss-gateway/conf/gateway.yaml)
for all settings and defaults.

### Security

Choose one caller authentication mode with `gateway.security.authentication`:

| Mode | Credential | Behavior |
| --- | --- | --- |
| `trust` (default) | Optional HTTP Basic username | No password verification; a missing or empty username uses the anonymous principal |
| `password` | HTTP Basic | Verifies the configured plaintext or bcrypt password |
| `token` | HTTP Bearer | Verifies a static token against its configured value or SHA-256 digest |
| `trusted-header` | Proxy identity header | Accepts an identity only from an explicitly allowed TCP peer |

Trust mode preserves anonymous access for the examples below. The name
`anonymous` is reserved and cannot be supplied as a caller identity in any mode.
Malformed credentials are rejected instead of becoming anonymous requests.

For password authentication on loopback:

```yaml
gateway.rest.listen: 127.0.0.1:8080
gateway.security.authentication: password
gateway.security.users: "alice:bcrypt:<hash>"
```

Generate a bcrypt record with a compatible tool such as `htpasswd -nB alice`,
and configure the hash after `alice:bcrypt:`. Plaintext entries such as
`alice:example-password` are also supported. Separate entries with commas;
commas in configured passwords are not supported. Keep credential files private.
Passwords verified with bcrypt must not exceed 72 bytes. The maximum accepted
cost is limited to 14, and at most four bcrypt verifications run at once.
Capacity exhaustion returns 429 with `Retry-After`. Request cancellation stops
waiting for a result; an already running bcrypt computation retains its capacity
until it finishes.

```bash
# curl prompts for the password instead of storing it in the shell command.
curl --user alice http://127.0.0.1:8080/v1/clusters
```

For token authentication, configure
`gateway.security.tokens: "sha256:<64-hex-digest>:alice"` and send
`Authorization: Bearer <token>`. Plaintext `<token>:alice` entries are supported;
prefer SHA-256 digests of high-entropy tokens. Bearer values use letters, digits,
`-._~+/`, and optional trailing `=` padding. Unknown or invalid credentials
return 401 with a challenge for the selected Basic or Bearer scheme. No provider
falls back to another mode. Static stores support up to 1,024 entries, principal
names up to 256 UTF-8 bytes, and authentication header values up to 8 KiB.
Invalid bcrypt records, invalid token syntax, and ambiguous mappings fail startup,
including entries that were only superficially checked before authentication was
implemented.

For a proxy that authenticates users and overwrites their identity header:

```yaml
gateway.security.authentication: trusted-header
gateway.security.trusted-header.name: x-forwarded-user
gateway.security.trusted-header.proxy-addresses: ["127.0.0.1", "::1"]
```

The allowlist accepts explicit IPv4/IPv6 addresses (also comma-separated for
environment overrides). It checks the actual TCP peer, never `X-Forwarded-For`
or `Forwarded`. The proxy must overwrite client-supplied identity headers and
its link to the Gateway must be trusted. Missing, ambiguous, or untrusted
identity assertions return 403; client Basic credentials cannot override that
policy. An IP allowlist does not authenticate the proxy cryptographically.

Native TLS termination is tracked in
[#4470](https://github.com/apache/fluss/issues/4470). Deploy network-facing
Gateway access behind a TLS ingress or load balancer. Password/token modes on
a non-loopback plaintext listener require
`gateway.security.allow-insecure-transport: true` for the trusted internal hop;
this is an explicit deployment choice, not validation of the proxy's TLS.
Loopback HTTP remains available for local development. The container listens on
`0.0.0.0`; restrict access to both REST and Prometheus ports with network controls.

All `/v1` business endpoints, cluster discovery, and `/v1/openapi.json` use the
selected mode. Health and readiness probes do not require credentials; the
separate metrics listener retains its existing management-network policy. The
served OpenAPI document describes the selected scheme; default trust declares
anonymous access or optional Basic credentials.

The Gateway uses one shared service connection per Fluss cluster. With
SASL/PLAIN, Fluss authorizes every request as the configured service account;
the HTTP caller principal is carried through Gateway request contexts but is
not forwarded as a Fluss login identity. Grant the service account only the
required permissions. User impersonation and user-specific connection pools
remain separate work.

### Health checks

`GET /health` reports process liveness. `GET /ready` reports whether the Gateway
accepts requests; it does not check Fluss connectivity. If Fluss is unavailable,
`/ready` can return HTTP 200 while a metadata, DDL, or write request returns HTTP
503 with `Retry-After`.

See [Health checks and graceful shutdown](../install-deploy/deploying-gateway.md#health-checks-and-graceful-shutdown)
for probe and drain behavior in supervised deployments.

## Create tables and write records

Set the endpoint and resource names used in the examples:

```bash
GATEWAY_URL=http://127.0.0.1:8080
CLUSTER=default
DATABASE=gateway_demo
```

### Create a database

```bash
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases" \
  -d "{\"database\":\"$DATABASE\"}"
```

### Create a log table and append records

Omit `primary_key` to create a log table:

```bash
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables" \
  -d '{
    "table_name": "events",
    "columns": [
      {"name": "event_id", "data_type": {"type": "BIGINT"}, "nullable": false},
      {"name": "message", "data_type": {"type": "STRING"}, "nullable": false}
    ],
    "distribution": {"bucket_count": 1, "bucket_keys": []}
  }'
```

Append rows whose fields match the table schema:

```bash
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/events/records" \
  -d '{
    "entries": [
      {"id": "event-1", "append": {"event_id": "1", "message": "created"}},
      {"id": "event-2", "append": {"event_id": "2", "message": "updated"}}
    ]
  }'
```

Use base-10 strings for `BIGINT` and `DECIMAL` values when JSON number
precision is insufficient.

### Create a primary-key table and modify records

Set `primary_key` to create a primary-key table. Bucket keys must be a subset of
the primary key:

```bash
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables" \
  -d '{
    "table_name": "users",
    "columns": [
      {"name": "user_id", "data_type": {"type": "INTEGER"}, "nullable": false},
      {"name": "name", "data_type": {"type": "STRING"}, "nullable": true},
      {"name": "note", "data_type": {"type": "STRING"}, "nullable": true}
    ],
    "primary_key": ["user_id"],
    "distribution": {"bucket_count": 1, "bucket_keys": ["user_id"]}
  }'
```

For a primary-key table without an auto-increment column, omitting
`partial_update_columns` makes `upsert` a full write. Omitted nullable columns
are written as null:

```bash
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/users/records" \
  -d '{
    "entries": [
      {"id": "user-1", "upsert": {"user_id": 1, "name": "Alice", "note": "active"}},
      {"id": "user-2", "upsert": {"user_id": 2, "name": "Bob", "note": "active"}}
    ]
  }'
```

Tables with an auto-increment column are an exception: `partial_update_columns`
is required, must include every primary-key column, and must not include the
auto-increment column. Omitting `partial_update_columns` or targeting the
auto-increment column returns HTTP 400.

For a partial update, list the primary-key and target columns. Every
non-primary-key, non-auto-increment column in the table must be nullable;
columns outside the list are preserved:

```bash
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/users/records" \
  -d '{
    "partial_update_columns": ["user_id", "note"],
    "entries": [
      {"id": "user-1-note", "upsert": {"user_id": 1, "note": "updated"}}
    ]
  }'
```

Delete a row by primary key:

```bash
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/users/records" \
  -d '{"entries": [{"id": "delete-user-2", "delete": {"user_id": 2}}]}'
```

### Check write results

A successful two-row batch returns:

```json
{
  "row_count": 2,
  "success_count": 2,
  "error_count": 0,
  "successes": [{"id": "user-1"}, {"id": "user-2"}],
  "failures": []
}
```

- Each entry must contain exactly one of `append`, `upsert`, or `delete`.
- The entry `id` must be unique within a request. It correlates outcomes but is
  not an idempotency key across requests.
- HTTP 200 can contain partial failures. Check both `successes` and `failures`.
- A schema validation error rejects the whole batch with HTTP 400 before any
  row is submitted.
- Delivery is at least once from the caller's perspective. Retrying can
  duplicate log appends, and a `timeout` outcome may already be applied.

The defaults are 10,000 rows and 32 MiB per request. The Gateway returns HTTP
413 when either limit is exceeded and HTTP 429 with `Retry-After` when write
admission or rate limits are exhausted.

## Inspect metadata and clean up

Describe the `users` table:

```bash
curl -sS --fail-with-body \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/users"
```

Gateway metadata APIs return schemas, not table records. Use a native Fluss
client to read records in this release.

Drop the tables before the database:

```bash
curl -sS --fail-with-body -X DELETE \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/users"
curl -sS --fail-with-body -X DELETE \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/events"
curl -sS --fail-with-body -X DELETE \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE"
```

Dropping a non-empty database returns HTTP 409.

## API reference

`GET /v1/openapi.json` returns the generated OpenAPI document. See the
[source specification](https://github.com/apache/fluss/blob/main/fluss-gateway/openapi.yaml)
for table alterations, partitions, data types, pagination, and errors.
