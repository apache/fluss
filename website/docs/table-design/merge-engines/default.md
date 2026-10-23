---
sidebar_label: Default (LastRow)
title: Default Merge Engine
sidebar_position: 2
---

# Default Merge Engine (LastRow)

## Overview

The **Default Merge Engine** behaves as a LastRow merge engine that retains the latest record for a given primary key. It supports all the operations: `INSERT`, `UPDATE`, `DELETE`.
Additionally, the default merge engine supports [Partial Update](table-design/table-types/pk-table.md#partial-update), which preserves the latest values for the specified update columns.
If the `'table.merge-engine'` property is not explicitly defined in the table properties when creating a Primary Key Table, the default merge engine will be applied automatically.


## Example

```sql title="Flink SQL"
CREATE TABLE T (
    k  INT,
    v1 DOUBLE,
    v2 STRING,
    PRIMARY KEY (k) NOT ENFORCED
);

-- Insert
INSERT INTO T(k, v1, v2) VALUES (1, 1.0, 't1');
INSERT INTO T(k, v1, v2) VALUES (1, 1.0, 't2');
SELECT * FROM T WHERE k = 1;
-- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 1  | 1.0 | t2 |
+----+-----+----+

-- Update
INSERT INTO T(k, v1, v2) VALUES (2, 2.0, 't2');
-- Switch to batch mode to perform update operation for UPDATE statement is only supported for batch mode currently
SET execution.runtime-mode = batch;
UPDATE T SET v1 = 4.0 WHERE k = 2;
SELECT * FROM T WHERE k = 2;
 -- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 2  | 4.0 | t2 |
+----+-----+----+


-- Partial Update
INSERT INTO T(k, v1) VALUES (3, 3.0); -- set v1 to 3.0
SELECT * FROM T WHERE k = 3;
-- Output:
+----+-----+------+
| k  | v1  | v2   |
+----+-----+------+
| 3  | 3.0 | null |
+----+-----+------+
INSERT INTO T(k, v2) VALUES (3, 't3'); -- set v2 to 't3'
SELECT * FROM T WHERE k = 3;
-- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 3  | 3.0 | t3 |
+----+-----+----+
 
-- Delete
DELETE FROM T WHERE k = 2;
-- Switch to streaming mode
SET execution.runtime-mode = streaming;
SELECT * FROM T;
-- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 1  | 1.0 | t2 |
+----+-----+----+
| 3  | 3.0 | t3 |
+----+-----+----+
```
## Ignoring NULL Values in Upserts

Set `table.merge-engine.default.ignore-nulls` to `true` to preserve existing values when an ordinary upsert supplies NULL for a target column. The default is `false`, which allows incoming NULLs to overwrite existing values.

```sql title="Flink SQL"
CREATE TABLE user_profile (
    user_id BIGINT,
    name STRING,
    city STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'table.merge-engine.default.ignore-nulls' = 'true'
);

INSERT INTO user_profile VALUES (1, 'Alice', CAST(NULL AS STRING));
INSERT INTO user_profile VALUES (1, CAST(NULL AS STRING), 'Hangzhou');
-- The stored row is (1, 'Alice', 'Hangzhou').
```

This is a table-level merge rule enforced by the server for all clients. It applies to ordinary full-row and partial-column upserts, including Flink INSERT and streaming UPDATE_AFTER records:

- Non-NULL values replace existing values in the target columns.
- NULL values preserve existing values in the target columns.
- Columns outside the partial-update target remain unchanged.
- When no row exists, nullable columns without a value remain NULL. A valid input with only non-NULL primary keys still inserts a row.
- Complex values such as ROW, ARRAY, and MAP are replaced as whole columns. Their nested NULLs are not merged recursively. Empty strings, empty collections, zero, and false are non-NULL values.
- Primary key, NOT NULL, generated-column, and partial-update constraints remain applicable.

An explicit SQL UPDATE retains assignment semantics, including `UPDATE user_profile SET city = NULL WHERE user_id = 1`. Java clients can use the existing `table.newUpsert().mergeMode(MergeMode.OVERWRITE)` API to overwrite values, including NULL. OVERWRITE still respects explicitly specified partial-update target columns.

Delete operations retain their existing behavior and remain controlled by `table.delete.behavior`. A full delete removes the row; a partial delete clears target columns and may remove the row according to the existing partial-delete rules. Ignoring NULLs does not prevent deletion. An upsert after deletion starts a new row and does not restore values from the deleted row.

The merge uses the existing processing order for each primary key, not event time. Batched writes retain the same per-column semantics as sequential writes. This option does not introduce aggregation undo recovery or strengthen existing ordering and recovery guarantees.

Both KV state and the changelog contain the complete merged row. FULL mode retains its before/after images; WAL mode records the merged after image. Preserving all logical values does not by itself guarantee that state writes or changelog records are suppressed. Existing TTL and generated-value rules continue to apply.

This option can only be configured when creating the table. It cannot be changed using ALTER TABLE. Its applicability follows the [merge engine option ownership rules](./index.md#merge-engine-options).
