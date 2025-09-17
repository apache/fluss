---
title: Upgrade Notes
sidebar_position: 3
---

# Upgrade Notes from v0.7 to v0.8

These upgrade notes discuss important aspects, such as configuration, behavior, or dependencies, that changed between Fluss 0.7 and Fluss 0.8. Please read these notes carefully if you are planning to upgrade your Fluss version to 0.8.

## Deprecation / End of Support

### Java 8 is Deprecated
Beginning with Fluss v0.8, we now only provide binary distributions built with Java 11.
**Java 8 is deprecated** as of this release and will be fully removed in future versions.

🔧 **For users still on Java 8**:
You can continue building Fluss from source using Java 8 by running:
```bash
mvn install -DskipTests -Pjava8
```
However, we **strongly recommend upgrading to Java 11 or higher** to ensure compatibility, performance, and long-term support.

🔁 **If you’re using Fluss with Apache Flink**:
Please also upgrade your Flink deployment to **Java 11 or above**. All Flink versions currently supported by Fluss are fully compatible with Java 11.

## Metrics Updates

We have updated the report level for some metrics and also removed some metrics, this greatly reduces the metrics amount and improves the performance.

The following metrics are removed:

- `fluss_tabletserver_table_bucket_inSyncReplicasCount` - Removed as redundant. Use `fluss_tabletserver_underReplicated` instead
- `fluss_tabletserver_table_bucket_log_size` - Removed as improvement. Use `fluss_tabletserver_table_bucket_logicalStorage_logSize` instead
- `fluss_tabletserver_table_bucket_kv_snapshot_latestSnapshotSize` - Removed as improvement. Use `fluss_tabletserver_table_bucket_logicalStorage_kvSize` instead

The following metrics are changed:

- The reporting level has been shifted from the `tableBucket` level to the `tabletserver` level:
  - `fluss_tabletserver_table_bucket_underMinIsr` to `fluss_tabletserver_underMinIsr`
  - `fluss_tabletserver_table_bucket_underReplicated` to `fluss_tabletserver_underReplicated`
  - `fluss_tabletserver_table_bucket_atMinIsr` to `fluss_tabletserver_atMinIsr`
  - `fluss_tabletserver_table_bucket_isrExpandsPerSecond` to `fluss_tabletserver_isrExpandsPerSecond`
  - `fluss_tabletserver_table_bucket_isrShrinksPerSecond` to `fluss_tabletserver_isrShrinksPerSecond`
  - `fluss_tabletserver_table_bucket_failedIsrUpdatesPerSecond` to `fluss_tabletserver_failedIsrUpdatesPerSecond`
  - `fluss_tabletserver_table_bucket_log_flushPerSecond` to `fluss_tabletserver_logFlushPerSecond`
  - `fluss_tabletserver_table_bucket_log_flushLatencyMs` to `fluss_tabletserver_logFlushLatencyMs`
  - `fluss_tabletserver_table_bucket_kv_preWriteBufferFlushPerSecond` to `fluss_tabletserver_kvFlushPerSecond`
  - `fluss_tabletserver_table_bucket_kv_preWriteBufferFlushLatencyMs` to `fluss_tabletserver_kvFlushLatencyMs`
  - `fluss_tabletserver_table_bucket_kv_preWriteBufferTruncateAsDuplicatedPerSecond` to `fluss_tabletserver_preWriteBufferTruncateAsDuplicatedPerSecond`
  - `fluss_tabletserver_table_bucket_kv_preWriteBufferTruncateAsErrorPerSecond` to `fluss_tabletserver_preWriteBufferTruncateAsErrorPerSecond`
- Correction addresses reporting errors in metric names by changing the `table` level metric prefix from `fluss_tabletserver_table__` (used a double underscore (__)) to `fluss_tabletserver_table_`. 
  - The affected metrics are all metrics with [Scope: tableserver, infix: table](docs/maintenance/observability/monitor-metrics.md#tablebucket)
  - For example, change `fluss_tabletserver_table__messagesInPerSecond` to `fluss_tabletserver_table_messagesInPerSecond`.