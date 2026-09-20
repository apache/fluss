---
sidebar_label: UpdateIfChanged
title: UpdateIfChanged Merge Engine
sidebar_position: 6
---

# UpdateIfChanged Merge Engine

The **UpdateIfChanged Merge Engine** keeps the last-row upsert semantics of the [Default Merge Engine](default.md)
but suppresses value-identical writes. By setting `'table.merge-engine' = 'update_if_changed'` in the table
properties, an upsert whose logical field values are all equal to the currently stored row becomes a no-op:
the stored row is kept and no changelog record is emitted. When at least one field differs, the incoming row
replaces the stored row and a normal update changelog is emitted.

This is useful for reducing unnecessary KV writes and changelog amplification when upstream data is replayed,
retried, or backfilled, and for making value-identical records that are replicated between primary-key tables a
no-op instead of producing new changelog records.

Compared with the other merge engines:

- `first_row` ignores every subsequent row for an existing primary key, including legitimate updates.
- `versioned` requires a version column and accepts rows whose version is equal to the stored version.
- `aggregation` applies field-level aggregation rather than last-row upsert semantics.

## Semantics

| Stored row | Incoming operation                  | Result                                                    |
|------------|-------------------------------------|-----------------------------------------------------------|
| Absent     | Insert/upsert                       | Store the incoming row and emit an insert changelog        |
| Present    | Every logical field is equal        | Keep the stored row and emit no changelog                  |
| Present    | At least one logical field differs  | Store the incoming row and emit the normal update changelog|
| Present    | Delete                              | Delete the row and emit a delete changelog                 |
| Absent     | Delete                              | No-op and emit no changelog                                |

Equality is based on logical field values, not raw serialized bytes:

- Null values compare using SQL/logical equality.
- Binary values are compared by content.
- Rows written with an older schema are aligned to the latest schema by stable column IDs. Fields that exist in
  the latest schema but are absent from a compared row are treated as null.
- For partial updates, the incoming update is first merged into a complete candidate row and then compared with
  the stored row.

:::note
This is value-based duplicate suppression, not version ordering. Replaying a row that is identical to the current
stored value is a no-op, while replaying a historical row whose fields differ from the current value is still
accepted. Unlike `first_row`, `versioned`, and `aggregation`, this engine supports `UPDATE`, `DELETE`, and Partial
Update, just like the default merge engine.
:::

## Example

```sql title="Flink SQL"
CREATE TABLE T (
    k  INT,
    v1 DOUBLE,
    v2 STRING,
    PRIMARY KEY (k) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'update_if_changed'
);

INSERT INTO T VALUES (1, 2.0, 't1');
-- value-identical upsert: no changelog is emitted
INSERT INTO T VALUES (1, 2.0, 't1');
-- a field differs: a normal update changelog is emitted
INSERT INTO T VALUES (1, 3.0, 't1');

SELECT * FROM T WHERE k = 1;

-- Output
-- +---+-----+------+
-- | k | v1  | v2   |
-- +---+-----+------+
-- | 1 | 3.0 | t1   |
-- +---+-----+------+
```
