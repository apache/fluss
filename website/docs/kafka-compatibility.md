---
sidebar_label: Kafka Compatibility
title: Kafka Compatibility
sidebar_position: 5.5
---

# Kafka Compatibility

Fluss can expose eligible Fluss tables as Kafka topics. Create the table in Fluss and enable its
Kafka mapping with `table.kafka.enabled=true`.

## Supported tables

Kafka mapping supports non-partitioned [Log Tables](table-design/table-types/log-table.md) with
the Arrow log format and no primary key. CREATE and ALTER validate the mapping before saving it.
All declared columns and nested value fields must be nullable, including ROW fields, ARRAY
elements, MAP values, and system columns. MAP keys retain Fluss's non-null constraint. The four
system columns are optional and can be declared independently.

## Cluster configuration

Enable Kafka on the cluster and configure a Kafka listener on each TabletServer. For a local
TabletServer:

```yaml title="server.yaml"
kafka.enabled: true
kafka.listener.names: KAFKA
kafka.default.database: fluss
bind.listeners: FLUSS://localhost:9123,KAFKA://localhost:9092
```

For distributed deployments, configure reachable `bind.listeners` and `advertised.listeners`.
The coordinator and all TabletServers must use the same `kafka.default.database`; see the
[server configuration reference](maintenance/configuration.md#common).

## SASL/PLAIN authentication

Kafka listeners support `PLAINTEXT` and `SASL_PLAINTEXT` with the `PLAIN` mechanism.
To require authentication, add the Kafka listener to the server's authentication protocol map:

```yaml title="server.yaml"
security.protocol.map: FLUSS:plaintext,KAFKA:sasl
security.sasl.enabled.mechanisms: PLAIN
security.sasl.plain.credentials: writer:change-me
```

The Kafka listener name must match `kafka.listener.names`. Credentials and dynamic credential
updates use the same settings as [native Fluss authentication](security/authentication.md).
Credential changes apply to new authentications; established connections retain their authenticated
identity. Kafka TLS, SCRAM, and re-authentication are not supported by this implementation.
`SASL_PLAINTEXT` does not encrypt credentials or record data.

Configure a Kafka producer with properties:

```properties
bootstrap.servers=localhost:9092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="writer" password="change-me";
enable.idempotence=false
```

After authentication, Kafka requests use the authenticated user's identity for Fluss ACL checks.
When authorization is enabled, the user must also have the required permissions on the target tables.
SASL handshake versions 0–1 and SaslAuthenticate versions 0–2 are supported. Ordinary requests are
rejected until authentication completes.

## Topic-to-table mapping

Topic names use `<database>.<table>`. Names without a database prefix use
`kafka.default.database` (default: `fluss`). With the default setting, mappings are:

| Kafka topic name | Fluss table path |
|---|---|
| `orders` | `fluss.orders` |
| `fluss.orders` | `fluss.orders` |
| `sales.orders` | `sales.orders` |
| `warehouse.events` | `warehouse.events` |

The native `fluss` database is created when the cluster has no databases. With Kafka enabled, the
coordinator also creates the configured default database if it is missing, even in a non-empty
cluster. Create other databases and all tables through Fluss; Kafka CreateTopics and DeleteTopics
are not supported.

A Kafka partition maps to the Fluss bucket with the same number, so the topic partition count must
match the table bucket count.

## Create or enable a table

The examples use an existing [Flink Fluss catalog](engine-flink/ddl.md#create-catalog). Create a
table for JSON values, optionally storing Kafka keys and headers:

```sql title="Flink SQL"
CREATE DATABASE IF NOT EXISTS sales;

CREATE TABLE sales.orders (
    order_id BIGINT,
    amount DECIMAL(18, 2),
    _kafka_key BYTES,
    _kafka_headers MAP<STRING, BYTES>
) WITH (
    'table.kafka.enabled' = 'true'
);
```

This maps the `sales.orders` topic to the table. JSON members map to `order_id` and `amount`; the
Kafka key and headers map to their system columns. With no conversion properties, values use
`parse_json` with the `flat` layout.

Enable Kafka mapping for an existing compatible table with:

```sql title="Flink SQL"
ALTER TABLE sales.orders SET (
    'table.kafka.enabled' = 'true'
);
```

Set the property to `false` to disable Kafka mapping. Disabled tables are not subject to Kafka
schema validation.

## Conversion properties

Set these table properties with CREATE or ALTER TABLE.

| Option | Type | Default | Description |
|---|---|---|---|
| `table.kafka.enabled` | Boolean | `false` | Enable Kafka mapping for the table. |
| `table.kafka.convert.value.type` | Enum | `parse_json` | Convert values as `binary`, `string`, or `parse_json`. |
| `table.kafka.convert.value.layout` | Enum | `flat` | JSON layout: `flat` maps top-level columns; `nested` maps fields in `_kafka_value ROW<...>`. Ignored for binary and string. |
| `table.kafka.convert.key.type` | Enum | None | Optional `binary` key conversion; requires `_kafka_key BYTES`. |

Declare `_kafka_key BYTES` to store Kafka keys as bytes; `table.kafka.convert.key.type` can be
omitted. To discard keys, omit both the column and the property.

| Value conversion | Required schema | Mapping |
|---|---|---|
| `binary` | `_kafka_value BYTES` | Store the value as bytes. |
| `string` | `_kafka_value STRING` | Decode the value as UTF-8. |
| `parse_json` with `flat` | Top-level business columns | Map JSON object members to columns by name. |
| `parse_json` with `nested` | `_kafka_value ROW<...>` | Map JSON members to fields in the value ROW. |

Binary, string, and nested JSON use `_kafka_value`; other top-level columns must be optional
system columns. Flat JSON uses top-level business columns and cannot declare `_kafka_value`.
JSON values must be objects. Nested ROW, ARRAY, and MAP schemas are supported. System names are
reserved at the table's top level, but are ordinary names inside business ROWs.

JSON schema limits:

- MAP keys must be STRING, including nested MAPs.
- Nesting depth is at most 64. Each top-level business field starts at depth zero; ROW fields,
  ARRAY elements, and MAP values each add one. The JSON root and `_kafka_value` container do not
  count.
- Fields must be nullable as described above. Value constraints are checked when records are
  decoded.

## System columns

Declare only the Kafka information the table needs to store. Unmapped keys, headers, and metadata
are not stored in row fields. `_kafka_value` is the conversion target described above.

| Column | Type | Mapping |
|---|---|---|
| `_kafka_key` | `BYTES` | Kafka record key. |
| `_kafka_headers` | `MAP<STRING, BYTES>` | Kafka headers; the last value wins for duplicate names. |
| `_kafka_metadata` | `ROW<timestamp TIMESTAMP_LTZ(3), timestamp_type INT, partition INT>` | Input timestamp, timestamp type, and requested partition. |
| `_kafka_rescue` | Nullable `STRING` | Compact JSON for unknown or unconvertible fields and flat-layout system-name conflicts. |

The metadata ROW must use the field names, order, and types shown above; the ROW and its fields
must be nullable. Timestamp type is `-1` when absent, `0` for CreateTime, or `1` for LogAppendTime.
An absent timestamp maps to null. Log offsets remain native log metadata and are not included in
this ROW.

Use the optional `_kafka_rescue` column to retain JSON fields that cannot be mapped to the table
schema for later inspection. It stores unknown fields, values that cannot be converted to the
declared type, and fields whose names conflict with system columns in the flat layout as compact
JSON. When a value cannot be converted, the corresponding table field is set to null while valid
fields are preserved. Without `_kafka_rescue`, these errors cause conversion to fail. Invalid JSON
and non-object roots always fail conversion. The column is null for binary/string conversion or
when there is nothing to rescue.
