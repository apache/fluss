---
sidebar_position: 8
---
# Filter Pushdown

Log scans can push a predicate down to the server, which uses per-batch column
statistics to skip whole Arrow record batches that cannot contain matching rows.
This reduces network transfer and decoding work, but it is not exact row
filtering: pruning works on whole batches, so the scan returns every batch that
may contain a matching record — including the non-matching rows in those
batches. Apply the filter again client-side when exact results are required.

## Requirements

- The table uses the ARROW log format (the default).
- Column statistics are enabled via the `table.statistics.columns` table
  property; without statistics no batches can be pruned.
- A Fluss v1.0+ cluster (older servers reject the property, and older consumers
  cannot parse the extended batch format it enables).

```rust
let descriptor = TableDescriptor::builder()
    .schema(/* ... */)
    // "*" collects statistics for all supported columns;
    // a comma-separated list like "id,name" restricts collection.
    .property("table.statistics.columns", "*")
    .build()?;
```

Statistics are collected for BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT,
FLOAT, DOUBLE, STRING, CHAR, DECIMAL, DATE, TIME, TIMESTAMP, and TIMESTAMP_LTZ
columns.

## Building Predicates

Predicates are built from column references created with
`fluss::predicate::col`:

```rust
use fluss::predicate::col;

// Comparisons: eq, ne, lt, le, gt, ge
let p = col("id").gt(100);
let p = col("name").eq("alice");

// Null checks
let p = col("opt").is_null();
let p = col("opt").is_not_null();

// Set membership
let p = col("id").is_in([1, 2, 3]);
let p = col("id").not_in([4, 5]);

// String matching
let p = col("name").starts_with("a");
let p = col("name").ends_with("z");
let p = col("name").contains("mid");

// Boolean combinations
let p = col("id").gt(100).and(col("name").starts_with("a"));
let p = col("id").lt(10).or(col("id").gt(100));
```

Literals convert from the natural Rust types (`i32`, `i64`, `f64`, `&str`,
`Decimal`, `TimestampNtz`, `TimestampLtz`, ...). `Literal::Date` holds epoch
days and `Literal::Time` milliseconds of day, matching Fluss's internal
encoding. The protocol has no negation node; negate with `ne` and `not_in`.

## Attaching a Filter to a Scan

```rust
use fluss::client::EARLIEST_OFFSET;
use fluss::predicate::col;
use std::time::Duration;

let scanner = table
    .new_scan()
    .filter(col("id").gt(100))?
    .create_log_scanner()?;
scanner.subscribe(0, EARLIEST_OFFSET).await?;
let records = scanner.poll(Duration::from_secs(5)).await?;
```

`filter` resolves the predicate against the table schema and errors on unknown
columns or literals the column's type cannot represent exactly. It is supported
by `create_log_scanner` and `create_record_batch_log_scanner`; the bounded
`create_bucket_batch_scanner` rejects it.

A runnable end-to-end example is in
[`crates/examples/src/example_filter_pushdown.rs`](https://github.com/apache/fluss/blob/main/fluss-rust/crates/examples/src/example_filter_pushdown.rs).
