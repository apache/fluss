---
id: sql-functions
title: SQL Functions
sidebar_label: SQL Functions
sidebar_position: 8
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SQL Functions

Apache Fluss registers a set of built-in RoaringBitmap SQL functions in `FlussCatalog`. These are
**Flink-side functions** that execute within the Flink query engine. They are distinct from the
storage-level `rbm32` / `rbm64` aggregators, which run inside the Fluss TabletServer during write.

## How to Use

After creating a Fluss catalog and switching to it, all functions are available in Flink SQL without
any `CREATE TEMPORARY FUNCTION` statement.

```sql
-- 1. Create the catalog
CREATE CATALOG fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

-- 2. Switch to it
USE CATALOG fluss_catalog;

-- 3. Use any bitmap function directly
SELECT rb_cardinality(rb_build(ARRAY[1, 2, 3, 2]));
-- Output: 3
```

All functions operate on `BYTES` columns containing standard 32-bit RoaringBitmap serialized data,
the same wire format used by the `rbm32` storage-level aggregator.

A typical workflow: use `rb_build_agg` to aggregate integer IDs into a bitmap during ingestion,
then use `rb_cardinality`, `rb_or_agg`, and the other scalar and aggregate functions at query time.

---

## Scalar Functions

Scalar functions operate on a single row and return a single value.

### rb_build

Builds a serialized `RoaringBitmap` from an `ARRAY<INT>` within a single row.

- **Signature:** `rb_build(values ARRAY<INT>) → BYTES`
- **Null Handling:** Returns `NULL` if the array argument is `NULL`. Null elements within the array are ignored. An empty or all-null element array returns an empty bitmap.

```sql
SELECT rb_cardinality(rb_build(ARRAY[1, 2, 3, 2]));
-- Output: 3  (duplicate 2 ignored)

SELECT rb_cardinality(rb_build(ARRAY[CAST(NULL AS INT), 1, 2]));
-- Output: 2  (null element ignored)

SELECT rb_build(CAST(NULL AS ARRAY<INT>)) IS NULL;
-- Output: TRUE
```

---

### rb_cardinality

Returns the number of distinct integers in a serialized `RoaringBitmap`.

- **Signature:** `rb_cardinality(bitmap BYTES) → BIGINT`
- **Null Handling:** Returns `NULL` for a null input. Returns `0` for an empty bitmap.

```sql
SELECT rb_cardinality(rb_build(ARRAY[1, 2, 3, 2]));
-- Output: 3

SELECT rb_cardinality(rb_build(ARRAY[CAST(NULL AS INT)]));
-- Output: 0  (empty bitmap)
```

---

### rb_contains

Returns whether a serialized `RoaringBitmap` contains a specific integer.

- **Signature:** `rb_contains(bitmap BYTES, value INT) → BOOLEAN`
- **Null Handling:** Returns `NULL` if either argument is `NULL`.

```sql
SELECT rb_contains(rb_build(ARRAY[1, 2, 3]), 2);
-- Output: TRUE

SELECT rb_contains(rb_build(ARRAY[1, 2, 3]), 5);
-- Output: FALSE
```

---

### rb_to_array

Converts a serialized `RoaringBitmap` to an `ARRAY<INT>` in ascending order.

- **Signature:** `rb_to_array(bitmap BYTES) → ARRAY<INT>`
- **Null Handling:** Returns `NULL` for a null input. Returns an empty array for an empty bitmap.

```sql
SELECT rb_to_array(rb_build(ARRAY[3, 1, 2]));
-- Output: [1, 2, 3]  (ascending order)
```

---

### rb_or

Returns the bitwise OR (union) of two serialized `RoaringBitmap` values.

- **Signature:** `rb_or(left BYTES, right BYTES) → BYTES`
- **Null Handling:** Returns `NULL` if either argument is `NULL`. To union bitmaps while ignoring nulls across rows, use `rb_or_agg`.

```sql
SELECT rb_cardinality(rb_or(rb_build(ARRAY[1, 2]), rb_build(ARRAY[2, 3])));
-- Output: 3  ({1, 2, 3})
```

---

### rb_and

Returns the bitwise AND (intersection) of two serialized `RoaringBitmap` values.

- **Signature:** `rb_and(left BYTES, right BYTES) → BYTES`
- **Null Handling:** Returns `NULL` if either argument is `NULL`. Returns an empty serialized bitmap (not `NULL`) when the intersection is empty.

```sql
SELECT rb_cardinality(rb_and(rb_build(ARRAY[1, 2, 3]), rb_build(ARRAY[2, 3, 4])));
-- Output: 2  ({2, 3})

SELECT rb_cardinality(rb_and(rb_build(ARRAY[1, 2]), rb_build(ARRAY[3, 4])));
-- Output: 0  (disjoint sets)
```

---

### rb_xor

Returns the bitwise XOR (symmetric difference) of two serialized `RoaringBitmap` values —
elements present in exactly one of the two inputs.

- **Signature:** `rb_xor(left BYTES, right BYTES) → BYTES`
- **Null Handling:** Returns `NULL` if either argument is `NULL`. Returns an empty serialized bitmap (not `NULL`) when the two inputs are identical.

```sql
SELECT rb_cardinality(rb_xor(rb_build(ARRAY[1, 2, 3]), rb_build(ARRAY[2, 3, 4])));
-- Output: 2  ({1, 4})

SELECT rb_cardinality(rb_xor(rb_build(ARRAY[1, 2]), rb_build(ARRAY[1, 2])));
-- Output: 0  (identical inputs cancel)
```

---

### rb_andnot

Returns elements present in the left bitmap but not in the right bitmap.

- **Signature:** `rb_andnot(left BYTES, right BYTES) → BYTES`
- **Null Handling:** Returns `NULL` if either argument is `NULL`. Returns an empty serialized bitmap (not `NULL`) when the right bitmap is a superset of the left.

```sql
SELECT rb_cardinality(rb_andnot(rb_build(ARRAY[1, 2, 3, 4]), rb_build(ARRAY[3, 4, 5])));
-- Output: 2  ({1, 2})

-- Users who visited page A but not page B
SELECT rb_cardinality(rb_andnot(a.uv_bitmap, b.uv_bitmap)) AS exclusive_visitors
FROM uv_agg a, uv_agg b
WHERE a.page_id = 1 AND b.page_id = 2 AND a.ymd = b.ymd;
```

---

## Aggregate Functions

Aggregate functions reduce multiple rows into a single bitmap result.

### rb_build_agg

Builds a serialized `RoaringBitmap` from a column of `INT` values across rows.

- **Signature:** `rb_build_agg(value INT) → BYTES`
- **Null Handling:** Null inputs are ignored. Returns `NULL` if all inputs are null.

```sql
SELECT rb_cardinality(rb_build_agg(user_id)) AS uv
FROM (VALUES (1), (2), (3), (2)) AS t(user_id);
-- Output: 3  (distinct users)
```

---

### rb_or_agg

Unions multiple serialized `RoaringBitmap` values via bitwise OR across rows.

- **Signature:** `rb_or_agg(bitmap BYTES) → BYTES`
- **Null Handling:** Null and empty inputs are ignored. Returns `NULL` if all inputs are null.

```sql
-- Roll up per-day bitmaps into a weekly unique visitor count
SELECT rb_cardinality(rb_or_agg(daily_bitmap)) AS weekly_uv
FROM (
    SELECT rb_build_agg(user_id) AS daily_bitmap
    FROM (VALUES (1, 1), (1, 2), (2, 2), (2, 3)) AS t(day, user_id)
    GROUP BY day
);
-- Output: 3  (users {1, 2, 3} across both days)
```

---

### rb_and_agg

Intersects multiple serialized `RoaringBitmap` values via bitwise AND across rows.

- **Signature:** `rb_and_agg(bitmap BYTES) → BYTES`
- **Null Handling:** Null and empty inputs are ignored. Returns `NULL` if the intersection is empty or all inputs are null.

```sql
-- Find users who appeared on every day
SELECT rb_cardinality(rb_and_agg(daily_bitmap)) AS retained_users
FROM (
    SELECT rb_build_agg(user_id) AS daily_bitmap
    FROM (VALUES (1, 1), (1, 2), (2, 2), (2, 3)) AS t(day, user_id)
    GROUP BY day
);
-- Output: 1  (only user 2 appeared on both days)
```

:::note
`rb_and_agg` has no server-side counterpart and executes entirely in Flink. Avoid combining
with `table.merge-engine=aggregation` on append-only streams.
:::

---

### rb_xor_agg

Aggregates multiple serialized `RoaringBitmap` values via bitwise XOR across rows.
Returns elements that appear in an **odd** number of input bitmaps.

- **Signature:** `rb_xor_agg(bitmap BYTES) → BYTES`
- **Null Handling:** Null and empty inputs are ignored. Returns `NULL` only when no non-null input has been accumulated (i.e. net count is zero). Returns an empty serialized bitmap when inputs cancel (e.g. two identical bitmaps XOR to empty) as long as at least one non-null input remains.

```sql
-- Find users who appeared on an odd number of days
SELECT rb_cardinality(rb_xor_agg(daily_bitmap)) AS changed_users
FROM (
    SELECT rb_build_agg(user_id) AS daily_bitmap
    FROM (VALUES (1, 1), (1, 2), (2, 2), (2, 3)) AS t(day, user_id)
    GROUP BY day
);
-- Output: 2  (users {1, 3} each appeared on exactly one day)
```

:::note
`rb_xor_agg` has no server-side counterpart and executes entirely in Flink. Unlike `rb_and_agg`,
it supports retraction on retractable streams (XOR is self-inverse).
:::

---

:::tip
For a full end-to-end tutorial including Docker setup and multi-dimensional roll-up queries,
see the [Real-Time UV Deduplication](https://fluss.apache.org/blog/roaringbitmap-uv-deduplication/) blog post.
:::