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

## Sequence Group

By default the latest write wins, whether or not it is actually the newest record. When several writers update the
same row, an out-of-order write silently overwrites values that are already newer.

A **sequence group** puts one or more columns under the order of a *sequence column*, so that those columns only take
an incoming value when the sequence column is not older than the stored one. Every group is arbitrated on its own, so
within a single write one group may move forward while another does not. This is what distinguishes a sequence group
from the [Versioned Merge Engine](table-design/merge-engines/versioned.md), which arbitrates the whole row with a
single version column.

A sequence group is declared with the `'fields.<sequence-column>.sequence-group'` property, whose value lists the
columns it protects:

```sql title="Flink SQL"
CREATE TABLE orders (
    order_id    BIGINT,
    pay_status  STRING,
    pay_time    BIGINT,
    ship_status STRING,
    ship_time   BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'fields.pay_time.sequence-group'  = 'pay_status',
    'fields.ship_time.sequence-group' = 'ship_status'
);

INSERT INTO orders VALUES (1, 'paid', 100, 'shipped', 100);

-- pay_time moves forward while ship_time falls behind,
-- so only the payment columns take the incoming values
INSERT INTO orders VALUES (1, 'refunded', 200, 'lost', 99);
SELECT * FROM orders;
-- Output:
+----------+------------+----------+-------------+-----------+
| order_id | pay_status | pay_time | ship_status | ship_time |
+----------+------------+----------+-------------+-----------+
| 1        | refunded   | 200      | shipped     | 100       |
+----------+------------+----------+-------------+-----------+

-- the shipping group catches up on its own, leaving the payment columns untouched
INSERT INTO orders VALUES (1, 'stale', 2, 'delivered', 300);
SELECT * FROM orders;
-- Output:
+----------+------------+----------+-------------+-----------+
| order_id | pay_status | pay_time | ship_status | ship_time |
+----------+------------+----------+-------------+-----------+
| 1        | refunded   | 200      | delivered   | 300       |
+----------+------------+----------+-------------+-----------+
```

Sequence groups apply to a full-row write as well as to a [Partial Update](table-design/table-types/pk-table.md#partial-update).

### Composite sequence key

Naming more than one sequence column declares a composite sequence key. The columns are compared in the declared
order, and the first one that differs decides:

```sql title="Flink SQL"
CREATE TABLE T (
    k     INT,
    v     STRING,
    epoch INT,
    ts    BIGINT,
    PRIMARY KEY (k) NOT ENFORCED
) WITH ('fields.epoch,ts.sequence-group' = 'v');
```

### Semantics

- A group takes the incoming values when its sequence columns are **not older** than the stored ones. Equal sequences
  advance, so a replayed record still refreshes the group.
- A group whose incoming sequence columns are **all NULL** carries no order information and is skipped.
- NULL orders before every value, so a stored NULL is the oldest sequence.
- A sequence column is arbitrated by the very group it orders, keeping its value in step with the columns it protects.
- `DELETE` is not arbitrated. A delete record carries the primary key alone and holds no sequence values to compare,
  so it removes the whole row.

### Restrictions

A table is rejected at creation when:

- it is a Log Table, or it configures any `'table.merge-engine'`, since no other merge engine consults the sequence
  groups while merging;
- a sequence column doesn't exist in the schema, or its type is not one of `INT`, `BIGINT`, `TIMESTAMP` and
  `TIMESTAMP_LTZ`;
- a primary key column is put into a group or used as a sequence column, since it holds the same value in both rows
  being merged;
- a sequence column is put into another group, since it reports the order of its own group;
- the same column is declared by more than one group.