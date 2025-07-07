---
title: Monitoring Metrics
sidebar_position: 3
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Monitoring Metrics

> Fluss exposes a comprehensive set of runtime metrics so you can observe the **health** and **performance** of every component, from JVM resources to per-table throughput.  
> This document explains **what** each metric measures and **how** to locate it in your monitoring backend (e.g., Prometheus, JMX).

---

## Metric Primitives

| Type          | Purpose                                                                                                                  | Example             |
|---------------|--------------------------------------------------------------------------------------------------------------------------|---------------------|
| **Gauge**     | Provides a value of any type at a point in time.                                                                         | JVM heap used       |
| **Counter**   | Used to count values by incrementing and decrementing.                                                                   | Total bytes written |
| **Histogram** | Measure the statistical distribution of a set of values including the min, max, mean, standard deviation and percentile. | Request latency     |
| **Meter**     | The gauge exports the meter's rate.                                                                                      | Records/second      |

---

## Metric Identifier & Scope
Every metric is assigned an identifier and a set of key-value pairs under which the metric will be reported.

The identifier is delimited by `metrics.scope.delimiter`. Currently, the `metrics.scope.delimiter` is not configurable, it determined by the metric reporter. Take prometheus as example, the scope will delimited by `_`, so the scope like `A_B_C`, while Fluss metrics will always begin with `fluss`, as `fluss_A_B_C`.

The key/value pairs are called variables and are used to filter metrics. There are no restrictions on the number of order of variables. Variables are case-sensitive.
```text
fluss_<scope>[_<infix>]<delimiter><metric-name>
```


1. Begins with `fluss_`.
2. **Scope** indicates *where* the metric originates (`coordinator`, `tabletserver`, `client`, …).
3. **Infix** (optional) groups related metrics (e.g., `status_JVM_CPU`).
4. `<delimiter>` depends on the reporter. For example Prometheus uses `_`.
5. Finally, the **metric name**.

**Example**

```text
fluss_tabletserver_status_JVM_CPU_load
```


represents the `recent JVM CPU load` of a `TabletServer`.

---

## Metric Reporters

Fluss ships reporters for Prometheus, JMX and more.  
See **[Metric Reporters](metric-reporters.md)** for setup details.

---

## Reference

### Cluster-level Metrics 
#### Scope: Coordinator scope

| Metric                    | Description                                | Type  |
|---------------------------|--------------------------------------------|-------|
| `activeCoordinatorCount`  | Active CoordinatorServers in the cluster.  | Gauge |
| `activeTabletServerCount` | Active TabletServers.                      | Gauge |
| `offlineBucketCount`      | Buckets currently offline.                 | Gauge |
| `tableCount`              | Total tables.                              | Gauge |
| `bucketCount`             | Total buckets.                             | Gauge |
| `replicasToDeleteCount`   | Replicas scheduled for deletion.           | Gauge |

---

### CPU Metrics 
#### Scope: ` Coordinator / TabletServer`, Infix: `status_JVM_CPU`

| Infix            | Metric | Description           | Type  |
|------------------|--------|-----------------------|-------|
| `status_JVM_CPU` | `load` | Recent JVM CPU usage. | Gauge |
|                  | `time` | Total JVM CPU time.   | Gauge |

---

### Memory Metrics 
#### Scope: `Coordinator / TabletServer`, Infix: `status_JVM_memory`

| Metric                 | Description                     | Type  |
|------------------------|---------------------------------|-------|
| `heap_used`            | Heap used (bytes).              | Gauge |
| `heap_committed`       | Heap committed (bytes).         | Gauge |
| `heap_max`             | Max heap available (bytes).     | Gauge |
| `nonHeap_used`         | Non-heap used (bytes).          | Gauge |
| `nonHeap_committed`    | Non-heap committed (bytes).     | Gauge |
| `nonHeap_max`          | Max non-heap (bytes).           | Gauge |
| `metaspace_used`       | Metaspace used (bytes).         | Gauge |
| `metaspace_committed`  | Metaspace committed (bytes).    | Gauge |
| `metaspace_max`        | Metaspace max (bytes).          | Gauge |
| `direct_count`         | Direct buffer count.            | Gauge |
| `direct_memoryUsed`    | Direct buffer memory (bytes).   | Gauge |
| `direct_totalCapacity` | Direct buffer capacity (bytes). | Gauge |
| `mapped_count`         | Mapped buffer count.            | Gauge |
| `mapped_memoryUsed`    | Mapped buffer memory (bytes).   | Gauge |
| `mapped_totalCapacity` | Mapped buffer capacity (bytes). | Gauge |

---

### Thread Metrics 
#### Scope: `Coordinator / TabletServer`, Infix: `status_JVM_threads`

| Metric  | Description  | Type  |
|---------|--------------|-------|
| `count` | Live threads | Gauge |

---

### Garbage Collection Metrics 
#### Scope: `Coordinator / TabletServer`, Infix: `status_JVM_GC`

| Metric                             | Description           | Type  |
|------------------------------------|-----------------------|-------|
| `<collector\|all>_count`           | Total GC collections. | Gauge |
| `<collector\|all>_time`            | Total GC time.        | Gauge |
| `<collector\|all>_timeMsPerSecond` | GC time (ms) per s.   | Gauge |

---

### Netty Metrics (
#### Scope: `Coordinator / TabletServer / Client`, Infix: `netty`

|  Metric                       | Description                  | Type  |
|-------------------------------|------------------------------|-------|
| `usedDirectMemory`            | Direct memory used by Netty. | Gauge |
| `numDirectArenas`             | Number of direct arenas.     | Gauge |
| `numAllocationsPerSecond`     | Allocations per second.      | Meter |
| `numHugeAllocationsPerSecond` | Huge allocations per second. | Meter |

---

### TabletServer Metrics

| Metric                                     | Description                                    | Type  |
|--------------------------------------------|------------------------------------------------|-------|
| `replicationBytesInPerSecond`              | Bytes written to follower replicas per second. | Meter |
| `replicationBytesOutPerSecond`             | Bytes read from leader replica per second.     | Meter |
| `leaderCount`                              | Leader replicas.                               | Gauge |
| `replicaCount`                             | Total replicas.                                | Gauge |
| `writerIdCount`                            | Writer IDs.                                    | Gauge |
| `delayedWriteCount`                        | Delayed writes.                                | Gauge |
| `delayedFetchCount`                        | Delayed fetch-log operations.                  | Gauge |
| `delayedWriteExpiresPerSecond`             | Expired delayed writes per second.             | Meter |
| `delayedFetchFromFollowerExpiresPerSecond` | Expired delayed fetches (follower) per second. | Meter |
| `delayedFetchFromClientExpiresPerSecond`   | Expired delayed fetches (client) per second.   | Meter |

---

### Request Metrics

#### Scope: `coordinator`, Infix: `request`

| Metric             | Description         | Type  |
|--------------------|---------------------|-------|
| `requestQueueSize` | Network queue size. | Gauge |

#### Scope: `tabletserver`, Infix: `request`

| Metric                 | Description                       | Type      |
|------------------------|-----------------------------------|-----------|
| `requestQueueSize`     | Network queue size.               | Gauge     |
| `requestsPerSecond`    | Requests per second *(per type)*. | Meter     |
| `errorsPerSecond`      | Errors per second *(per type)*.   | Meter     |
| `requestBytes`         | Request-size distribution.        | Histogram |
| `totalTimeMs`          | End-to-end latency.               | Histogram |
| `requestProcessTimeMs` | Processing time.                  | Histogram |
| `requestQueueTimeMs`   | Queue wait time.                  | Histogram |
| `responseSendTimeMs`   | Response send time.               | Histogram |

> **Request types**  
> `request_productLog`, `request_putKv`, `request_lookup`, `request_prefixLookup`, `request_metadata`

#### Scope: `client`, Infix: `request`

| Metric               | Description                | Type  |
|----------------------|----------------------------|-------|
| `bytesInPerSecond`   | Bytes received per second. | Gauge |
| `bytesOutPerSecond`  | Bytes sent per second.     | Meter |
| `requestsPerSecond`  | Requests per second.       | Meter |
| `responsesPerSecond` | Responses per second.      | Meter |
| `requestLatencyMs`   | Request latency.           | Gauge |
| `requestsInFlight`   | In-flight requests.        | Gauge |

---

### Table Metrics 
#### Scope: `tabletserver`, Infix: `table`

| Metric                                    | Description                                   | Type  |
|-------------------------------------------|-----------------------------------------------|-------|
| `messagesInPerSecond`                     | Messages written per second.                  | Meter |
| `bytesInPerSecond`                        | Bytes written per second.                     | Meter |
| `bytesOutPerSecond`                       | Bytes read per second.                        | Meter |
| `totalProduceLogRequestsPerSecond`        | Produce-log requests per second.              | Meter |
| `failedProduceLogRequestsPerSecond`       | Failed produce-log requests per second.       | Meter |
| `totalFetchLogRequestsPerSecond`          | Fetch-log requests per second.                | Meter |
| `failedFetchLogRequestsPerSecond`         | Failed fetch-log requests per second.         | Meter |
| `totalPutKvRequestsPerSecond`             | PUT-kv requests per second.                   | Meter |
| `failedPutKvRequestsPerSecond`            | Failed PUT-kv requests per second.            | Meter |
| `totalLookupRequestsPerSecond`            | Lookup requests per second.                   | Meter |
| `failedLookupRequestsPerSecond`           | Failed lookup requests per second.            | Meter |
| `totalLimitScanRequestsPerSecond`         | Limit-scan requests per second.               | Meter |
| `failedLimitScanRequestsPerSecond`        | Failed limit-scan requests per second.        | Meter |
| `totalPrefixLookupRequestsPerSecond`      | Prefix-lookup requests per second.            | Meter |
| `failedPrefixLookupRequestsPerSecond`     | Failed prefix-lookup requests per second.     | Meter |
| `remoteLogCopyBytesPerSecond`             | Bytes copied to remote per second.            | Meter |
| `remoteLogCopyRequestsPerSecond`          | Remote log-copy requests per second.          | Meter |
| `remoteLogCopyErrorPerSecond`             | Failed remote log-copy requests per second.   | Meter |
| `remoteLogDeleteRequestsPerSecond`        | Remote log-delete requests per second.        | Meter |
| `remoteLogDeleteErrorPerSecond`           | Failed remote log-delete requests per second. | Meter |

---

### Bucket Metrics 
#### Scope:`tabletserver` Infix: `table_bucket`

| Metric                     | Description                                | Type  |
|----------------------------|--------------------------------------------|-------|
| `inSyncReplicasCount`      | In-sync replicas for the bucket.           | Gauge |
| `underMinIsr`              | `1` if below *min ISR*; otherwise `0`.     | Gauge |
| `underReplicated`          | `1` if below replication factor.           | Gauge |
| `atMinIsr`                 | `1` if exactly at *min ISR*.               | Gauge |
| `isrExpandsPerSecond`      | ISR expansions per second.                 | Meter |
| `isrShrinksPerSecond`      | ISR shrinks per second.                    | Meter |
| `failedIsrUpdatesPerSecond`| Failed ISR updates per second.             | Meter |

#### Bucket Log Metrics 
#### Scope: `tabletserver` Infix: `table_bucket_log`

| Metric              | Description                           | Type      |
|---------------------|---------------------------------------|-----------|
| `numSegments`       | Segments in local storage.            | Gauge     |
| `endOffset`         | End offset in local storage.          | Gauge     |
| `size`              | Log size in local storage (bytes).    | Gauge     |
| `flushPerSecond`    | Log flushes per second.               | Meter     |
| `flushLatencyMs`    | Log-flush latency (ms).               | Histogram |

#### Remote-Log Metrics 
#### Scope: `tabletserver`, Infix: `table_bucket_remoteLog`

| Metric        | Description                    | Type  |
|---------------|--------------------------------|-------|
| `numSegments` | Segments in remote storage.    | Gauge |
| `endOffset`   | End offset in remote storage.  | Gauge |
| `size`        | Total remote log size (bytes). | Gauge |

#### KV Buffer Metrics 
#### Scope: `tabletserver`, Infix: `table_bucket_kv`

| Metric                                         | Description                                            | Type      |
|------------------------------------------------|--------------------------------------------------------|-----------|
| `preWriteBufferFlushPerSecond`                 | Pre-write buffer flushes per second.                   | Meter     |
| `preWriteBufferFlushLatencyMs`                 | Buffer-flush latency (ms).                             | Histogram |
| `preWriteBufferTruncateAsDuplicatedPerSecond`  | Buffer truncations due to duplicates per second.       | Meter     |
| `preWriteBufferTruncateAsErrorPerSecond`       | Buffer truncations due to errors per second.           | Meter     |

#### KV Snapshot Metrics 
#### Scope: `tabletserver`, Infix: `table_bucket_kv_snapshot`

| Metric               | Description                         | Type  |
|----------------------|-------------------------------------|-------|
| `latestSnapshotSize` | Latest KV snapshot size (bytes).    | Gauge |

---

## Flink Connector Standard Metrics

Fluss implements [FLIP-33](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics) to expose common source and sink metrics.

### Source Metrics

| Metric                      | Level                  | Description                                            | Type  |
|-----------------------------|------------------------|--------------------------------------------------------|-------|
| `currentEmitEventTimeLag`   | Flink Source Operator  | Time between emitting the record and file creation.    | Gauge |
| `currentFetchEventTimeLag`  | Flink Source Operator  | Time between reading the data file and file creation.  | Gauge |

### Sink Metrics

| Metric                   | Level | Description                | Type    |
|--------------------------|-------|----------------------------|---------|
| `numBytesOut`            | Table | Total output bytes.        | Counter |
| `numBytesOutPerSecond`   | Table | Output bytes per second.   | Meter   |
| `numRecordsOut`          | Table | Total output records.      | Counter |
| `numRecordsOutPerSecond` | Table | Output records per second. | Meter   |

---

### Further Reading

* **[Metric Reporters](metric-reporters.md)** – Configure Prometheus, JMX and other sinks.
* **[Flink Metrics](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/metrics/#system-metrics)** – Complete reference for Flink-side metrics.
