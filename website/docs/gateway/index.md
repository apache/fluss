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

## Capabilities and limitations

| Area | Operations |
| --- | --- |
| Service | Health, readiness, cluster discovery, and OpenAPI 3.1 |
| Metadata | List databases, tables, and partitions; describe tables |
| Database DDL | Create and drop databases |
| Table DDL | Create, validate, alter, and drop tables |
| Partition DDL | Add and drop partitions |
| Records | Batch append, upsert, partial update, and delete |

The 1.0 preview does not support HTTP caller authentication, end-user identity
propagation, primary-key or prefix lookup, log scans, or other record reads.

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

The 1.0 preview implements only `trust` mode: the REST listener does not
authenticate callers or terminate TLS. `password`, `token`, and
`trusted-header` are reserved values and do not protect requests.

Deploy the Gateway behind an authenticated ingress that terminates TLS. The
container listens on `0.0.0.0`; restrict access to both the REST and Prometheus
ports with network controls.

The Gateway uses one shared service connection per Fluss cluster. With
SASL/PLAIN, Fluss authorizes every request as the configured service account;
the HTTP caller identity is not forwarded. Grant the service account only the
required permissions and keep its credentials out of images and source control.

### Health checks

`GET /health` reports process liveness. `GET /ready` reports whether the Gateway
accepts requests; it does not check Fluss connectivity. If Fluss is unavailable,
`/ready` can return HTTP 200 while a metadata, DDL, or write request returns HTTP
503 with `Retry-After`.

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
