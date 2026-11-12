---
title: Sketch SQL Functions
sidebar_position: 9
---

# Sketch SQL Functions

With `org.apache.fluss.spark.FlussSparkSessionExtensions` enabled, the connector provides functions
for serialized Apache DataSketches HLL and KLL doubles sketches. They run in Spark, independently
of the storage-side Fluss aggregation merge engines.

| Function | Input | Result |
| --- | --- | --- |
| `fluss_hll_build_agg(value)` | Integer or string | Serialized HLL bytes |
| `fluss_hll_merge_agg(sketch)` | HLL binary | Unioned HLL bytes |
| `fluss_hll_estimate(sketch)` | HLL binary | Approximate distinct count (`DOUBLE`) |
| `fluss_kll_double_build_agg(value)` | Numeric, converted to `DOUBLE` | Serialized KLL doubles bytes |
| `fluss_kll_double_merge_agg(sketch)` | KLL doubles binary | Merged KLL doubles bytes |
| `fluss_kll_double_quantile(sketch, rank)` | KLL doubles binary, rank in `[0, 1]` | Quantile (`DOUBLE`) |

Spark 3.5 already provides `hll_sketch_agg`, `hll_union_agg`, `hll_union`, and
`hll_sketch_estimate`. Prefer those when only Spark 3.5 is needed. The Fluss HLL build helper
delegates to Spark's built-in on 3.5 and provides a fallback on 3.4. Native names are not replaced.
Do not substitute Spark `approx_count_distinct` state for DataSketches HLL bytes.

HLL uses `lgK=12`; merging lower-precision sketches can reduce precision. KLL uses `k=200`.
Use consistent HLL input types across writers: a numeric ID and its string representation are
different inputs. KLL follows DataSketches handling of NaN values (ignored).

```sql
SELECT fluss_hll_estimate(fluss_hll_build_agg(user_id)) AS unique_users
FROM user_events;

SELECT fluss_kll_double_quantile(
    fluss_kll_double_build_agg(CAST(latency_ms AS DOUBLE)), 0.95) AS p95
FROM requests;

-- Merge stored sketches before extracting the result.
SELECT fluss_kll_double_quantile(fluss_kll_double_merge_agg(latency_kll), 0.95)
FROM daily_latency_metrics;
```

Aggregates ignore null inputs. KLL empty aggregates return null; HLL build follows the Spark
built-in's serialized empty-sketch behavior. Readers return null for null or empty sketches.
Invalid bytes, wrong sketch families and ranks outside `[0, 1]` fail the query.

These aggregates support batch queries and Spark's distributed partial aggregation. They do not
support subtracting previous observations. KLL batches are not idempotent: replaying a batch adds
its observations again. KLL write-side merging requires a Fluss server with `kll_double_sketch`
support. Spark `approx_percentile` returns values, not compatible serialized KLL state.
