---
title: "Ordered Index Reads"
sidebar_position: 2
---

# Ordered Index Reads

Live KV scans and lookups on an ordered-key primary-key table return rows in
index key order within one index bucket. `ReadDirection.FORWARD` traverses
ascending encoded keys, and
`ReadDirection.REVERSE` traverses descending encoded keys. The direction reverses
all remaining index fields, including the primary-key suffix distinguishing
rows with equal range values. The current implementation uses the primary-key
index. The same contract applies to secondary-index access paths when supported.

## Table requirements

Set `table.kv.ordered-key.enabled=true` when creating the primary-key table. It
defaults to false and is immutable because it selects the persisted index key
encoding. Upgrade every participating client and server before enabling it.
The option is independent of `table.kv.format-version`. Supported key types are
scalar types except FLOAT and DOUBLE. Null key values and constructed key types
are rejected.

For a range read, equality fields must identify a physical primary-key prefix
containing every bucket field. Include every partition field as an equality
condition. The range column immediately follows the physical prefix after
partition fields are excluded. Scan values follow the supplied equality column
list. Lookup columns follow the existing `lookupBy` prefix rules.

## Query configuration

`Scan.direction(ReadDirection)` and `Lookup.direction(ReadDirection)` return
immutable configurations. Repeated calls replace the direction. Refinements
preserve it regardless of configuration order. The default direction for an
ordered-key single-bucket read is `FORWARD`.

Explicit direction requires `table.kv.ordered-key.enabled=true`. A request on a
table without this property fails when the scanner or lookuper is created.
Ordinary reads on such tables retain their existing contracts. A primary-key
point lookup accepts direction on an ordered-key table and returns at most one
row. Typed lookup uses the same configuration and result order.

`Range` describes logical lower and upper endpoints independently of direction.
The equality prefix and immediately following range column select a supported
index access path. Both directions read the same matching set. Open, closed,
single-sided and empty intervals have the same meaning in either direction.
Each finite endpoint is a one-field `InternalRow` with a non-null value in the
range column's internal type. Reversed endpoints are invalid. Equal endpoints
include all rows with that value only when both endpoints are closed. A missing
partition or empty interval returns no rows.

For a table with primary key `(tenant_id, user_id, event_time, id)` distributed
by `(tenant_id, user_id)`, the following reads the same interval in reverse.
The tenant and user columns are INT, and event_time is BIGINT.

```java
import org.apache.fluss.client.table.ReadDirection;
import org.apache.fluss.utils.Range;

List<String> prefixColumns = Arrays.asList("tenant_id", "user_id");
InternalRow prefixValues = GenericRow.of(1, 2);
Range<GenericRow> bounds = Range.closedOpen(GenericRow.of(0L), GenericRow.of(1000L));

Lookuper lookuper = table.newLookup()
        .lookupBy("tenant_id", "user_id")
        .rangeBy("event_time")
        .direction(ReadDirection.REVERSE)
        .createLookuper();
LookupResult result = lookuper.lookup(prefixValues, bounds).get();

try (BatchScanner scanner = table.newScan()
        .equalTo(prefixColumns, prefixValues)
        .range("event_time", bounds)
        .direction(ReadDirection.REVERSE)
        .limit(100)
        .createBatchScanner()) {
    while (true) {
        CloseableIterator<InternalRow> batch = scanner.pollBatch(Duration.ofSeconds(5));
        if (batch == null) {
            break;
        }
        try (CloseableIterator<InternalRow> rows = batch) {
            while (rows.hasNext()) {
                InternalRow row = rows.next();
                // Process the row.
            }
        }
    }
}
```

## Scan contract

A scanner holds its direction, iterator and fixed KV snapshot for its lifetime.
The concatenation of all returned batches follows the selected index order.
Projection and schema decoding preserve that order. Concurrent later writes are
invisible. Loss of the scan session fails the read.

The limit selects the first N matching rows in the chosen direction across all
batches. Zero returns no rows and negative limits are rejected for ordered KV
reads. A full single-bucket ordered KV scan supports direction and limit without
requiring range conditions. For example, open a reverse scan of an explicit
bucket and consume it with the same polling loop shown above.

```java
Scan scan = table.newScan().direction(ReadDirection.REVERSE).limit(100);
try (BatchScanner scanner = scan.createBatchScanner(bucket)) {
    // Consume batches with pollBatch using the loop above.
}
```

Close this scanner after consumption, including when stopping before its limit.
`createBatchScanner()` without range conditions is also supported when the table
resolves to one bucket across all partitions. With range conditions,
`createBatchScanner(bucket)` restricts the routed query to that bucket. A
disjoint bucket returns no rows, and a bucket from another table is invalid.

An explicit ordered request must resolve to at most one index bucket. A request
requiring multiple index buckets fails before opening scanners. An ordinary
multi-bucket scan has no global ordering guarantee. Log and snapshot-ID scanners
reject explicit direction.

## Lookup contract

Each invocation returns every matching row in index order in
`LookupResult.getRowList()`. Lookup has no limit. A batched RPC can mix directions
and bounds, with each query retaining its own result order and response position.
Filtering and schema decoding preserve relative order. Each invocation fixes a
request-local iterator view. Separate invocations and retries can observe
different views. Results are materialized in memory, so lookup is intended for
small result sets. Use a scanner for paginated reads of larger ranges. A ranged
lookuper requires `lookup(key, bounds)`. A lookuper without `rangeBy` rejects
that overload. Range lookup cannot be combined with insert-if-not-exists.

## Storage and protocol contract

Both directions use the physical interval `[L, U)`. Forward reads start at the
first key greater than or equal to L. Reverse reads start at the last key strictly
less than U and stop below L. Prefix lookup intersects supplied bounds with its
encoded prefix interval before iterating.

SCAN_KV v1 carries optional direction on the open request. Continuations use the
session direction. PREFIX_LOOKUP v2 carries optional direction per query. Both
explicit forward and reverse requests require support for their protocol version
and ordered keys. An unsupported server fails the request before data is read.
