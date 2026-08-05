---
id: sql-functions
title: SQL Functions
sidebar_label: SQL Functions
sidebar_position: 7
---

# SQL Functions

Apache Fluss registers a set of built-in SQL functions in `FlussCatalog`. These are **Flink-side
functions** that execute within the Flink query engine. They are distinct from the storage-level
`rbm32` / `rbm64` aggregators, which execute inside the Fluss TabletServer during write.

## How to Use

After creating a Fluss catalog and setting it as the active catalog, all functions are available
in Flink SQL without any `CREATE TEMPORARY FUNCTION` statement.

```sql
-- 1. Create the catalog (replace connection details as needed)
CREATE CATALOG fluss_catalog WITH (
    'type'         = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

-- 2. Switch to the catalog
USE CATALOG fluss_catalog;

-- 3. Use any bitmap function directly
SELECT rb_cardinality(rb_build(ARRAY[1, 2, 3, 2]));
-- Output: 3
```

All functions below operate on `BYTES` columns containing standard 32-bit RoaringBitmap
serialized data — the same wire format used by the `rbm32` storage-level aggregator.

---

## Aggregate Functions

Aggregate functions reduce multiple rows into a single bitmap result.

### rb_build_agg

Builds a serialized `RoaringBitmap` from a column of `INT` values across rows.

**Signature:** `rb_build_agg(value INT) → BYTES`

| Input | Output |
|---|---|
| NULL | ignored |
| all NULL | `NULL` |

```sql
-- Input
-- user_id
-- 1
-- 2
-- 3
-- 2  (duplicate)

SELECT rb_cardinality(rb_build_agg(user_id)) AS uv
FROM (VALUES (1), (2), (3), (2)) AS t(user_id);

-- Output: 3
```

---

### rb_or_agg

Unions multiple serialized `RoaringBitmap` values via bitwise OR across rows.

**Signature:** `rb_or_agg(bitmap BYTES) → BYTES`

| Input | Output |
|---|---|
| NULL rows | ignored |
| all NULL | `NULL` |

```sql
-- Input: two bitmaps {1,2} and {2,3}
SELECT rb_cardinality(rb_or_agg(bmap)) AS result
FROM (
    VALUES
        (X'3A30000000000000020000000102'),  -- bitmap {1, 2}
        (X'3A30000000000000020000000203')   -- bitmap {2, 3}
) AS t(bmap);

-- Output: 3  ({1, 2, 3})
```

:::tip
Use `rb_or_agg` to roll up per-day bitmaps to a weekly unique visitor count.
:::

---

### rb_and_agg

Intersects multiple serialized `RoaringBitmap` values via bitwise AND across rows.

**Signature:** `rb_and_agg(bitmap BYTES) → BYTES`

| Input | Output |
|---|---|
| NULL rows | ignored |
| empty intersection | `NULL` |
| all NULL | `NULL` |

```sql
-- Input: two bitmaps {1,2,3} and {2,3,4}
SELECT rb_cardinality(rb_and_agg(bmap)) AS result
FROM (
    VALUES
        (X'3A3000000000000003000000010203'),  -- bitmap {1, 2, 3}
        (X'3A3000000000000003000000020304')   -- bitmap {2, 3, 4}
) AS t(bmap);

-- Output: 2  ({2, 3})
```

:::note
`rb_and_agg` has no server-side counterpart and executes entirely in Flink.
Avoid combining with `table.merge-engine=aggregation` on append-only streams.
:::

---

### rb_xor_agg

Aggregates multiple serialized `RoaringBitmap` values via bitwise XOR across rows.
Returns elements that appear in an **odd** number of input bitmaps.

**Signature:** `rb_xor_agg(bitmap BYTES) → BYTES`

| Input | Output |
|---|---|
| NULL rows | ignored |
| inputs that cancel (e.g. two identical bitmaps) | empty bitmap (not `NULL`) |
| all NULL | `NULL` |

```sql
-- Input: two bitmaps {1,2,3} and {2,3,4}
SELECT rb_cardinality(rb_xor_agg(bmap)) AS result
FROM (
    VALUES
        (X'3A3000000000000003000000010203'),  -- bitmap {1, 2, 3}
        (X'3A3000000000000003000000020304')   -- bitmap {2, 3, 4}
) AS t(bmap);

-- Output: 2  ({1, 4})
```

:::note
`rb_xor_agg` has no server-side counterpart and executes entirely in Flink.
Unlike `rb_and_agg`, it supports retraction (XOR is self-inverse).
:::

---

## Scalar Functions

Scalar functions operate on a single row and return a single value.

### rb_cardinality

Returns the number of distinct integers in a serialized `RoaringBitmap`.

**Signature:** `rb_cardinality(bitmap BYTES) → BIGINT`

| Input | Output |
|---|---|
| `NULL` | `NULL` |
| empty bitmap | `0` |

```sql
SELECT rb_cardinality(rb_build(ARRAY[1, 2, 3, 2]));
-- Output: 3
```

---

### rb_build

Builds a serialized `RoaringBitmap` from an `ARRAY<INT>` within a single row.

**Signature:** `rb_build(values ARRAY<INT>) → BYTES`

| Input | Output |
|---|---|
| `NULL` array | `NULL` |
| empty array `ARRAY[]` | empty bitmap |
| array with null elements | null elements ignored |

```sql
SELECT rb_cardinality(rb_build(ARRAY[1, 2, 3, 2]));
-- Output: 3  (duplicate 2 ignored)

SELECT rb_cardinality(rb_build(ARRAY[CAST(NULL AS INT), 1, 2]));
-- Output: 2  (null element ignored)

SELECT rb_build(CAST(NULL AS ARRAY<INT>));
-- Output: NULL
```

---

### rb_contains

Returns whether a serialized `RoaringBitmap` contains a specific integer.

**Signature:** `rb_contains(bitmap BYTES, value INT) → BOOLEAN`

| Input | Output |
|---|---|
| either argument `NULL` | `NULL` |

```sql
SELECT rb_contains(rb_build(ARRAY[1, 2, 3]), 2);
-- Output: TRUE

SELECT rb_contains(rb_build(ARRAY[1, 2, 3]), 5);
-- Output: FALSE
```

---

### rb_to_array

Converts a serialized `RoaringBitmap` to an `ARRAY<INT>` in ascending order.

**Signature:** `rb_to_array(bitmap BYTES) → ARRAY<INT>`

| Input | Output |
|---|---|
| `NULL` | `NULL` |
| empty bitmap | `[]` |

```sql
SELECT rb_to_array(rb_build(ARRAY[3, 1, 2]));
-- Output: [1, 2, 3]  (ascending order)
```

---

### rb_or

Returns the bitwise OR (union) of two serialized `RoaringBitmap` values.

**Signature:** `rb_or(left BYTES, right BYTES) → BYTES`

| Input | Output |
|---|---|
| either argument `NULL` | `NULL` |

```sql
SELECT rb_cardinality(rb_or(rb_build(ARRAY[1, 2]), rb_build(ARRAY[2, 3])));
-- Output: 3  ({1, 2, 3})
```

To merge bitmaps while ignoring nulls across rows, use `rb_or_agg`.

---

### rb_and

Returns the bitwise AND (intersection) of two serialized `RoaringBitmap` values.

**Signature:** `rb_and(left BYTES, right BYTES) → BYTES`

| Input | Output |
|---|---|
| either argument `NULL` | `NULL` |
| empty intersection | empty bitmap (not `NULL`) |

```sql
SELECT rb_cardinality(rb_and(rb_build(ARRAY[1, 2, 3]), rb_build(ARRAY[2, 3, 4])));
-- Output: 2  ({2, 3})

SELECT rb_cardinality(rb_and(rb_build(ARRAY[1, 2]), rb_build(ARRAY[3, 4])));
-- Output: 0  (disjoint sets → empty bitmap)
```

---

### rb_xor

Returns the bitwise XOR (symmetric difference) of two serialized `RoaringBitmap` values.
Elements present in exactly one of the two inputs.

**Signature:** `rb_xor(left BYTES, right BYTES) → BYTES`

| Input | Output |
|---|---|
| either argument `NULL` | `NULL` |
| identical inputs | empty bitmap (not `NULL`) |

```sql
SELECT rb_cardinality(rb_xor(rb_build(ARRAY[1, 2, 3]), rb_build(ARRAY[2, 3, 4])));
-- Output: 2  ({1, 4})

SELECT rb_cardinality(rb_xor(rb_build(ARRAY[1, 2]), rb_build(ARRAY[1, 2])));
-- Output: 0  (identical inputs cancel)
```

---

### rb_andnot

Returns elements present in the left bitmap but **not** in the right bitmap.

**Signature:** `rb_andnot(left BYTES, right BYTES) → BYTES`

| Input | Output |
|---|---|
| either argument `NULL` | `NULL` |
| right is superset of left | empty bitmap (not `NULL`) |

```sql
SELECT rb_cardinality(rb_andnot(rb_build(ARRAY[1, 2, 3, 4]), rb_build(ARRAY[3, 4, 5])));
-- Output: 2  ({1, 2})

-- Users who visited page A but not page B
SELECT rb_cardinality(rb_andnot(a.uv_bitmap, b.uv_bitmap)) AS exclusive_visitors
FROM uv_agg a, uv_agg b
WHERE a.page_id = 1 AND b.page_id = 2 AND a.ymd = b.ymd;
```

:::tip
For a full end-to-end tutorial including Docker setup and multi-dimensional roll-up queries,
see the [Real-Time UV Deduplication](https://fluss.apache.org/blog/roaringbitmap-uv-deduplication/) blog post.
:::