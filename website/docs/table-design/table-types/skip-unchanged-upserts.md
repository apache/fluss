---
sidebar_label: Skip Unchanged Upserts
title: Skip Unchanged Upserts
sidebar_position: 2
---

# Skip Unchanged Upserts

Set `'table.upsert.skip-unchanged' = 'true'` to avoid redundant KV state writes and changelog records when an upsert
leaves the stored row unchanged. The option defaults to `false`.
The [default merge engine](/table-design/merge-engines/default.md) still produces the complete
candidate row, including when an incoming write is a [partial update](pk-table.md#partial-update). Fluss then compares
that candidate with the stored row. When their logical values are equal, Fluss keeps the stored row and emits no
changelog record. When they differ, Fluss stores the candidate and emits the normal changelog records.

This is useful for reducing unnecessary KV writes and changelog amplification when upstream data is replayed,
retried, or backfilled, and for making value-identical records that are replicated between primary-key tables a
no-op instead of producing new changelog records.

## Semantics

| Stored row | Incoming operation                 | Result                                                     |
|------------|------------------------------------|------------------------------------------------------------|
| Absent     | Insert/upsert                      | Store the incoming row and emit an insert changelog         |
| Present    | Candidate row is logically equal   | Keep the stored row and emit no changelog                   |
| Present    | Candidate row logically differs    | Store the candidate and emit the normal update changelog    |
| Present    | Full-row delete                    | Delete the row and emit a delete changelog                  |
| Absent     | Full-row delete                    | No-op and emit no changelog                                 |

The delete rows assume `table.delete.behavior = 'allow'` (the default). With `ignore`, Fluss skips deletes; with
`disable`, it rejects them.

Equality is based on logical field values, not raw serialized bytes:

- Two null values are equal; a null and a non-null value differ.
- Binary values are compared by content.
- Arrays are compared element by element, nested rows recursively, and maps by their key-value entries regardless of
  entry order. Floating-point `+0.0` and `-0.0` compare as equal.
- Rows written with an older schema are aligned to the latest schema by stable column IDs. Fields that exist in
  the latest schema but are absent from a compared row are treated as null.
- For partial updates, the incoming update is first merged into a complete candidate row and then compared with
  the stored row.
- A partial delete removes the whole row if all non-target columns are null. Otherwise, it clears the target columns
  and emits no changelog record if the resulting row is unchanged.

:::note
This is value-based duplicate suppression, not version ordering. Replaying a row that is identical to the current
stored value is a no-op, while replaying a historical row whose fields differ from the current value is still
accepted. The option preserves the default merge engine's `UPDATE`, `DELETE`, and partial-update behavior.
:::

## Requirements and write cost

This option is supported only for primary key tables with no `table.merge-engine` setting and
`'table.changelog.image' = 'FULL'` (the default). It cannot be combined with `table.kv.ttl`.
Writes using `MergeMode.OVERWRITE` are rejected while this option is enabled.
Set it when creating the table; it cannot be changed with `ALTER TABLE`.
Both processing-time and event-time row-level TTL are rejected. With processing-time TTL, skipping an unchanged write
would prevent the row expiration time from refreshing. `WAL` image mode is rejected because finding unchanged values
requires a lookup of the stored row, removing the full-row WAL write optimization.

In the required `FULL` changelog image mode, each upsert already looks up the existing value while holding the write
lock. When a row exists, this option also compares it with the complete candidate row. The comparison and its setup
add costs even to updates that change the row. Measure throughput and latency with your workload before enabling it
on a write-heavy table.

## Example

```sql title="Flink SQL"
CREATE TABLE T (
    k  INT,
    v1 DOUBLE,
    v2 STRING,
    PRIMARY KEY (k) NOT ENFORCED
) WITH (
    'table.upsert.skip-unchanged' = 'true'
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
