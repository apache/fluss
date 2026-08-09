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

Create a Fluss catalog first. Its built-in functions are available without any
`CREATE TEMPORARY FUNCTION` statement.

```sql
CREATE CATALOG fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);
```

You can either make the Fluss catalog current or reference its functions by fully qualified name.

<Tabs>
<TabItem value="current-catalog" label="Current Fluss Catalog" default>

Switch to the Fluss catalog when the session mainly works with Fluss objects. Function names can
then be used directly.

```sql
USE CATALOG fluss_catalog;

SELECT rb_cardinality(rb_build(ARRAY[1, 2, 3, 2]));
-- Output: 3
```

</TabItem>
<TabItem value="qualified-name" label="Fully Qualified Names">

When working across multiple catalogs, keep the current catalog unchanged and qualify each Fluss
function as `<catalog>.<database>.<function>`.

```sql
SELECT fluss_catalog.fluss.rb_cardinality(
    fluss_catalog.fluss.rb_build(ARRAY[1, 2, 3, 2])
);
-- Output: 3
```

`fluss` is the default database of `FlussCatalog`. You can replace it with another Fluss database
name, but that database must already exist and the current user must have permission to access it.

</TabItem>
</Tabs>

All functions operate on `BYTES` columns containing standard 32-bit RoaringBitmap serialized data,
the same wire format used by the `rbm32` storage-level aggregator.

## Example: Windowed Bitmap Batches

RoaringBitmap SQL functions and the `rbm32` merge engine complement each other. A common pattern is
to use `rb_build_agg` to turn each short processing-time window into a bitmap, write those bitmap
batches to a Fluss primary-key table, and let the `rbm32` merge engine union batches with the same
key. Downstream queries can use `rb_cardinality` to read the cumulative distinct count.

The following example assumes that the Fluss catalog contains an append-only streaming table named
`click_events` with columns `page_id BIGINT`, `user_id INT`, and `proc_time AS PROCTIME()`.

The `page_uv` table uses the [Aggregation Merge Engine](/table-design/merge-engines/aggregation.md#rbm32)
by setting `table.merge-engine=aggregation`. The `fields.uv_bitmap.agg=rbm32` option enables `rbm32`
aggregation on `uv_bitmap`, which unions the serialized 32-bit RoaringBitmap values written for the
same primary key.

```sql
USE CATALOG fluss_catalog;

-- Store one cumulative bitmap per page in Fluss.
CREATE TABLE page_uv (
    page_id  BIGINT,
    uv_bitmap BYTES,
    PRIMARY KEY (page_id) NOT ENFORCED
) WITH (
    'table.merge-engine'      = 'aggregation',
    'fields.uv_bitmap.agg'    = 'rbm32'
);

-- Build one bitmap per page every five seconds. Each completed window writes
-- another bitmap batch, and rbm32 unions it into the existing page bitmap.
INSERT INTO page_uv
SELECT page_id,
       rb_build_agg(user_id) AS uv_bitmap
FROM TABLE(
    TUMBLE(
        TABLE click_events,
        DESCRIPTOR(proc_time),
        INTERVAL '5' SECOND
    )
)
GROUP BY page_id, window_start, window_end;

-- Query the UV value on pages
SELECT page_id,
       rb_cardinality(uv_bitmap) AS uv
FROM page_uv
WHERE page_id = 1;
```

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
    VALUES
        (1, rb_build(ARRAY[1, 2])),
        (2, rb_build(ARRAY[2, 3]))
) AS t(day_id, daily_bitmap);
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
    VALUES
        (1, rb_build(ARRAY[1, 2])),
        (2, rb_build(ARRAY[2, 3]))
) AS t(day_id, daily_bitmap);
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
    VALUES
        (1, rb_build(ARRAY[1, 2])),
        (2, rb_build(ARRAY[2, 3]))
) AS t(day_id, daily_bitmap);
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
