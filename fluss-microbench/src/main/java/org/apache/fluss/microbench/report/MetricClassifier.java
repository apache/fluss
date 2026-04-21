/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.microbench.report;

import org.apache.fluss.microbench.sampling.ResourceSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** Metric classification, labeling, grouping, and lookup logic extracted from ReportWriter. */
class MetricClassifier {

    private MetricClassifier() {}

    static final Map<String, String> LABEL_MAP = buildLabelMap();

    /**
     * Classifies a server metric key into a logical display group. Returns a pair of [groupId,
     * displayTitle].
     */
    static String[] classifyMetric(String key) {
        String lower = key.toLowerCase();
        String dim = inferDimension(key);

        // Server JVM
        if (lower.contains("jvm.cpu")) {
            return new String[] {"server.jvm.cpu", "Server JVM / CPU"};
        }
        if (lower.contains("jvm.gc")) {
            return new String[] {"server.jvm.gc" + dim, "Server JVM / GC" + dimLabel(dim)};
        }
        if (lower.contains("jvm.memory")) {
            return new String[] {"server.jvm.memory", "Server JVM / Memory"};
        }
        if (lower.contains("jvm.threads") || lower.contains("classloader")) {
            return new String[] {"server.jvm.threads", "Server JVM / Threads"};
        }

        // RocksDB
        if (lower.contains("rocksdb") && lower.contains("memory")) {
            return new String[] {"server.kv.rocksdb.memory", "Server KV / RocksDB Memory"};
        }
        if (lower.contains("rocksdb") && (lower.contains("latency") || lower.contains("stall"))) {
            return new String[] {"server.kv.rocksdb.latency", "Server KV / RocksDB Latency"};
        }
        if (lower.contains("rocksdb")
                && (lower.contains("bytes")
                        || lower.contains("compaction")
                        || lower.contains("flush"))) {
            return new String[] {"server.kv.rocksdb.io", "Server KV / RocksDB I/O"};
        }
        if (lower.contains("rocksdb")) {
            return new String[] {"server.kv.rocksdb.other", "Server KV / RocksDB Other"};
        }

        // TabletServer table-level
        if (key.startsWith("tabletserver.table.")) {
            String rest = key.substring("tabletserver.table.".length());
            if (rest.startsWith("bucket.log")) {
                return new String[] {"server.log.bucket", "Server Log / Bucket"};
            }
            if (rest.startsWith("bucket.")) {
                return new String[] {"server.kv.bucket", "Server KV / Bucket"};
            }
            if (rest.contains("Kv")
                    || rest.contains("kv")
                    || rest.contains("Lookup")
                    || rest.contains("lookup")
                    || rest.contains("PrefixLookup")
                    || rest.contains("LimitScan")
                    || rest.contains("PutKv")) {
                return new String[] {
                    "server.kv.ops" + dim, "Server KV / Operations" + dimLabel(dim)
                };
            }
            if (rest.contains("Log")
                    || rest.contains("log")
                    || rest.contains("Produce")
                    || rest.contains("Fetch")) {
                return new String[] {
                    "server.log.ops" + dim, "Server Log / Operations" + dimLabel(dim)
                };
            }
            if (rest.contains("bytesIn")
                    || rest.contains("bytesOut")
                    || rest.contains("messagesIn")) {
                return new String[] {
                    "server.table.throughput" + dim, "Server / Throughput" + dimLabel(dim)
                };
            }
            if (rest.contains("remoteLog")) {
                return new String[] {
                    "server.log.remote" + dim, "Server Log / Remote Storage" + dimLabel(dim)
                };
            }
            return new String[] {
                "server.table.other" + dim, "Server Table / Other" + dimLabel(dim)
            };
        }

        // TabletServer top-level
        if (key.startsWith("tabletserver.")) {
            String rest = key.substring("tabletserver.".length());
            if (rest.contains("bytesIn")
                    || rest.contains("bytesOut")
                    || rest.contains("messagesIn")) {
                return new String[] {
                    "server.table.throughput" + dim, "Server / Throughput" + dimLabel(dim)
                };
            }
            if (rest.contains("Flush") || rest.contains("flush")) {
                return new String[] {"server.flush" + dim, "Server / Flush" + dimLabel(dim)};
            }
            if (rest.contains("replication")) {
                return new String[] {
                    "server.replication" + dim, "Server / Replication" + dimLabel(dim)
                };
            }
            if (rest.contains("Isr") || rest.contains("isr")) {
                return new String[] {"server.isr" + dim, "Server / ISR" + dimLabel(dim)};
            }
            if (rest.contains("delayed")) {
                return new String[] {
                    "server.delayed" + dim, "Server / Delayed Ops" + dimLabel(dim)
                };
            }
            if (rest.contains("preWriteBuffer")) {
                return new String[] {
                    "server.writebuffer" + dim, "Server / Write Buffer" + dimLabel(dim)
                };
            }
            if (rest.contains("netty")) {
                return new String[] {"server.netty", "Server / Netty"};
            }
            if (rest.contains("request.")) {
                return new String[] {
                    "server.request" + dim, "Server / Request Processing" + dimLabel(dim)
                };
            }
            if (rest.contains("logicalStorage") || rest.contains("physicalStorage")) {
                return new String[] {"server.storage", "Server / Storage"};
            }
            return new String[] {"server.other" + dim, "Server / Other" + dimLabel(dim)};
        }

        // Coordinator
        if (key.startsWith("coordinator.")) {
            if (lower.contains("event")) {
                return new String[] {
                    "coordinator.event" + dim, "Coordinator / Event" + dimLabel(dim)
                };
            }
            if (lower.contains("netty")) {
                return new String[] {"coordinator.netty", "Coordinator / Netty"};
            }
            if (lower.contains("table")
                    || lower.contains("bucket")
                    || lower.contains("partition")) {
                return new String[] {"coordinator.table", "Coordinator / Table Management"};
            }
            if (lower.contains("lake") || lower.contains("tiering")) {
                return new String[] {"coordinator.lake", "Coordinator / Lake Tiering"};
            }
            return new String[] {"coordinator.general", "Coordinator / General"};
        }

        // Client
        if (key.startsWith("client.")) {
            if (lower.contains("netty")) {
                return new String[] {"client.netty", "Internal Client / Netty"};
            }
            return new String[] {"client.rpc", "Internal Client / RPC"};
        }

        return new String[] {"other", "Other"};
    }

    /** Returns a dimension suffix for sub-grouping metrics by statistical type. */
    static String inferDimension(String key) {
        if (key.endsWith(".rate")) {
            return ".rate";
        }
        if (key.endsWith(".count") && key.contains("PerSecond")) {
            return ".cumul";
        }
        if (key.endsWith(".p50")
                || key.endsWith(".p95")
                || key.endsWith(".p99")
                || key.endsWith(".mean")
                || key.endsWith(".max")) {
            return ".pct";
        }
        if (key.endsWith(".count") && !key.contains("PerSecond")) {
            return ".pct";
        }
        return "";
    }

    /** Returns a human-readable label suffix for the dimension. */
    static String dimLabel(String dimSuffix) {
        switch (dimSuffix) {
            case ".rate":
                return " (Rate)";
            case ".cumul":
                return " (Cumulative)";
            case ".pct":
                return " (Latency)";
            default:
                return "";
        }
    }

    private static Map<String, String> buildLabelMap() {
        Map<String, String> m = new LinkedHashMap<>();
        // RocksDB
        m.put("rocksdbMemoryUsageTotal", "Total Memory");
        m.put("rocksdbBlockCacheMemoryUsageTotal", "Block Cache");
        m.put("rocksdbBlockCachePinnedUsageTotal", "Block Cache Pinned");
        m.put("rocksdbBytesReadTotal", "Bytes Read (cumul.)");
        m.put("rocksdbBytesWrittenTotal", "Bytes Written (cumul.)");
        m.put("rocksdbCompactionBytesReadTotal", "Compaction Read (cumul.)");
        m.put("rocksdbCompactionBytesWrittenTotal", "Compaction Written (cumul.)");
        m.put("rocksdbCompactionPendingMax", "Compaction Pending");
        m.put("rocksdbCompactionTimeMicrosMax", "Compaction Time Max (us)");
        m.put("rocksdbFlushBytesWrittenTotal", "Flush Written (cumul.)");
        m.put("rocksdbFlushPendingMax", "Flush Pending");
        m.put("rocksdbGetLatencyMicrosMax", "Get Latency Max (us)");
        m.put("rocksdbMemTableMemoryUsageTotal", "MemTable Memory");
        m.put("rocksdbMemTableUnFlushedMemoryUsageTotal", "MemTable Unflushed");
        m.put("rocksdbNumFilesAtLevel0Max", "L0 Files");
        m.put("rocksdbTableReadersMemoryUsageTotal", "Table Readers Memory");
        m.put("rocksdbWriteLatencyMicrosMax", "Write Latency Max (us)");
        m.put("rocksdbWriteStallMicrosMax", "Write Stall Max (us)");
        // Throughput
        m.put("bytesInPerSecond.count", "Bytes In (cumul.)");
        m.put("bytesInPerSecond.rate", "Bytes In Rate");
        m.put("bytesOutPerSecond.count", "Bytes Out (cumul.)");
        m.put("bytesOutPerSecond.rate", "Bytes Out Rate");
        m.put("messagesInPerSecond.count", "Messages In (cumul.)");
        m.put("messagesInPerSecond.rate", "Messages In Rate");
        // KV Operations
        m.put("totalPutKvRequestsPerSecond.count", "Put KV Total");
        m.put("totalPutKvRequestsPerSecond.rate", "Put KV Rate");
        m.put("failedPutKvRequestsPerSecond.count", "Put KV Failed");
        m.put("failedPutKvRequestsPerSecond.rate", "Put KV Failed Rate");
        m.put("totalLookupRequestsPerSecond.count", "Lookup Total");
        m.put("totalLookupRequestsPerSecond.rate", "Lookup Rate");
        m.put("failedLookupRequestsPerSecond.count", "Lookup Failed");
        m.put("failedLookupRequestsPerSecond.rate", "Lookup Failed Rate");
        m.put("totalPrefixLookupRequestsPerSecond.count", "Prefix Lookup Total");
        m.put("totalPrefixLookupRequestsPerSecond.rate", "Prefix Lookup Rate");
        m.put("failedPrefixLookupRequestsPerSecond.count", "Prefix Lookup Failed");
        m.put("failedPrefixLookupRequestsPerSecond.rate", "Prefix Lookup Failed Rate");
        m.put("totalLimitScanRequestsPerSecond.count", "Limit Scan Total");
        m.put("totalLimitScanRequestsPerSecond.rate", "Limit Scan Rate");
        m.put("failedLimitScanRequestsPerSecond.count", "Limit Scan Failed");
        m.put("failedLimitScanRequestsPerSecond.rate", "Limit Scan Failed Rate");
        // Log Operations
        m.put("totalFetchLogRequestsPerSecond.count", "Fetch Log Total");
        m.put("totalFetchLogRequestsPerSecond.rate", "Fetch Log Rate");
        m.put("failedFetchLogRequestsPerSecond.count", "Fetch Log Failed");
        m.put("failedFetchLogRequestsPerSecond.rate", "Fetch Log Failed Rate");
        m.put("totalProduceLogRequestsPerSecond.count", "Produce Log Total");
        m.put("totalProduceLogRequestsPerSecond.rate", "Produce Log Rate");
        // Flush
        m.put("kvFlushPerSecond.count", "KV Flush (cumul.)");
        m.put("kvFlushPerSecond.rate", "KV Flush Rate");
        m.put("kvFlushLatencyMs.count", "KV Flush Count");
        m.put("kvFlushLatencyMs.mean", "KV Flush Latency Avg (ms)");
        m.put("kvFlushLatencyMs.p50", "KV Flush Latency p50 (ms)");
        m.put("kvFlushLatencyMs.p95", "KV Flush Latency p95 (ms)");
        m.put("kvFlushLatencyMs.p99", "KV Flush Latency p99 (ms)");
        m.put("kvFlushLatencyMs.max", "KV Flush Latency Max (ms)");
        m.put("logFlushPerSecond.count", "Log Flush (cumul.)");
        m.put("logFlushPerSecond.rate", "Log Flush Rate");
        m.put("logFlushLatencyMs.count", "Log Flush Count");
        m.put("logFlushLatencyMs.mean", "Log Flush Latency Avg (ms)");
        m.put("logFlushLatencyMs.p50", "Log Flush Latency p50 (ms)");
        m.put("logFlushLatencyMs.p95", "Log Flush Latency p95 (ms)");
        m.put("logFlushLatencyMs.p99", "Log Flush Latency p99 (ms)");
        m.put("logFlushLatencyMs.max", "Log Flush Latency Max (ms)");
        // Replication
        m.put("replicationBytesInPerSecond.count", "Repl Bytes In (cumul.)");
        m.put("replicationBytesInPerSecond.rate", "Repl Bytes In Rate");
        m.put("replicationBytesOutPerSecond.count", "Repl Bytes Out (cumul.)");
        m.put("replicationBytesOutPerSecond.rate", "Repl Bytes Out Rate");
        // ISR
        m.put("isrExpandsPerSecond.count", "ISR Expands (cumul.)");
        m.put("isrExpandsPerSecond.rate", "ISR Expands Rate");
        m.put("isrShrinksPerSecond.count", "ISR Shrinks (cumul.)");
        m.put("isrShrinksPerSecond.rate", "ISR Shrinks Rate");
        m.put("failedIsrUpdatesPerSecond.count", "ISR Update Failures");
        m.put("failedIsrUpdatesPerSecond.rate", "ISR Update Failure Rate");
        // Delayed
        m.put("delayedFetchCount", "Delayed Fetch Count");
        m.put("delayedWriteCount", "Delayed Write Count");
        m.put("delayedWriteExpiresPerSecond.count", "Write Expires (cumul.)");
        m.put("delayedWriteExpiresPerSecond.rate", "Write Expires Rate");
        m.put("delayedFetchFromClientExpiresPerSecond.count", "Client Fetch Expires (cumul.)");
        m.put("delayedFetchFromClientExpiresPerSecond.rate", "Client Fetch Expires Rate");
        m.put("delayedFetchFromFollowerExpiresPerSecond.count", "Follower Fetch Expires (cumul.)");
        m.put("delayedFetchFromFollowerExpiresPerSecond.rate", "Follower Fetch Expires Rate");
        // Write Buffer
        m.put("preWriteBufferTruncateAsDuplicatedPerSecond.count", "Truncate Dup (cumul.)");
        m.put("preWriteBufferTruncateAsDuplicatedPerSecond.rate", "Truncate Dup Rate");
        m.put("preWriteBufferTruncateAsErrorPerSecond.count", "Truncate Error (cumul.)");
        m.put("preWriteBufferTruncateAsErrorPerSecond.rate", "Truncate Error Rate");
        // Server Request
        m.put("requestsPerSecond.count", "Requests (cumul.)");
        m.put("requestsPerSecond.rate", "Requests Rate");
        m.put("errorsPerSecond.count", "Errors (cumul.)");
        m.put("errorsPerSecond.rate", "Error Rate");
        m.put("requestQueueSize", "Request Queue Size");
        m.put("requestProcessTimeMs.mean", "Process Time Avg (ms)");
        m.put("requestProcessTimeMs.p50", "Process Time p50 (ms)");
        m.put("requestProcessTimeMs.p95", "Process Time p95 (ms)");
        m.put("requestProcessTimeMs.p99", "Process Time p99 (ms)");
        m.put("requestProcessTimeMs.max", "Process Time Max (ms)");
        m.put("requestProcessTimeMs.count", "Processed Count");
        m.put("requestQueueTimeMs.mean", "Queue Time Avg (ms)");
        m.put("requestQueueTimeMs.p50", "Queue Time p50 (ms)");
        m.put("requestQueueTimeMs.p95", "Queue Time p95 (ms)");
        m.put("requestQueueTimeMs.p99", "Queue Time p99 (ms)");
        m.put("requestQueueTimeMs.max", "Queue Time Max (ms)");
        m.put("requestQueueTimeMs.count", "Queued Count");
        m.put("requestBytes.mean", "Request Size Avg (B)");
        m.put("requestBytes.p50", "Request Size p50 (B)");
        m.put("requestBytes.p95", "Request Size p95 (B)");
        m.put("requestBytes.p99", "Request Size p99 (B)");
        m.put("requestBytes.max", "Request Size Max (B)");
        m.put("requestBytes.count", "Request Count");
        m.put("responseSendTimeMs.mean", "Response Send Avg (ms)");
        m.put("responseSendTimeMs.p99", "Response Send p99 (ms)");
        m.put("responseSendTimeMs.max", "Response Send Max (ms)");
        m.put("responseSendTimeMs.count", "Response Count");
        m.put("totalTimeMs.mean", "Total Time Avg (ms)");
        m.put("totalTimeMs.p50", "Total Time p50 (ms)");
        m.put("totalTimeMs.p95", "Total Time p95 (ms)");
        m.put("totalTimeMs.p99", "Total Time p99 (ms)");
        m.put("totalTimeMs.max", "Total Time Max (ms)");
        m.put("totalTimeMs.count", "Total Count");
        // Netty
        m.put("numAllocationsPerSecond.count", "Allocs (cumul.)");
        m.put("numAllocationsPerSecond.rate", "Alloc Rate");
        m.put("numDirectArenas", "Direct Arenas");
        m.put("numHugeAllocationsPerSecond.count", "Huge Allocs (cumul.)");
        m.put("numHugeAllocationsPerSecond.rate", "Huge Alloc Rate");
        m.put("usedDirectMemory", "Direct Memory Used");
        // JVM
        m.put("CPU.load", "CPU Load (%)");
        m.put("CPU.time", "CPU Time (ms)");
        m.put("threads.count", "Thread Count");
        m.put("classesLoaded", "Classes Loaded");
        m.put("classesUnloaded", "Classes Unloaded");
        m.put("heap.used", "Heap Used");
        m.put("heap.committed", "Heap Committed");
        m.put("heap.max", "Heap Max");
        m.put("nonHeap.used", "Non-Heap Used");
        m.put("nonHeap.committed", "Non-Heap Committed");
        m.put("metaspace.used", "Metaspace Used");
        m.put("metaspace.committed", "Metaspace Committed");
        m.put("direct.memoryUsed", "Direct Buffer Used");
        m.put("direct.totalCapacity", "Direct Buffer Capacity");
        m.put("direct.count", "Direct Buffer Count");
        m.put("mapped.memoryUsed", "Mapped Buffer Used");
        m.put("mapped.totalCapacity", "Mapped Buffer Capacity");
        m.put("mapped.count", "Mapped Buffer Count");
        // GC
        m.put("G1 Young Generation.count", "Young GC Count");
        m.put("G1 Young Generation.time", "Young GC Time (ms)");
        m.put("G1 Old Generation.count", "Old GC Count");
        m.put("G1 Old Generation.time", "Old GC Time (ms)");
        m.put("all.count", "Total GC Count");
        m.put("all.time", "Total GC Time (ms)");
        m.put("timeMsPerSecond.count", "GC Time Total (ms)");
        m.put("timeMsPerSecond.rate", "GC Time Rate (ms/s)");
        // Coordinator Event
        m.put("eventProcessingTimeMs.count", "Events Processed");
        m.put("eventProcessingTimeMs.mean", "Processing Avg (ms)");
        m.put("eventProcessingTimeMs.p50", "Processing p50 (ms)");
        m.put("eventProcessingTimeMs.p95", "Processing p95 (ms)");
        m.put("eventProcessingTimeMs.p99", "Processing p99 (ms)");
        m.put("eventProcessingTimeMs.max", "Processing Max (ms)");
        m.put("eventQueueSize", "Event Queue Size");
        m.put("eventQueueTimeMs.count", "Events Queued");
        m.put("eventQueueTimeMs.mean", "Queue Wait Avg (ms)");
        m.put("eventQueueTimeMs.p50", "Queue Wait p50 (ms)");
        m.put("eventQueueTimeMs.p95", "Queue Wait p95 (ms)");
        m.put("eventQueueTimeMs.p99", "Queue Wait p99 (ms)");
        m.put("eventQueueTimeMs.max", "Queue Wait Max (ms)");
        // Coordinator general
        m.put("activeCoordinatorCount", "Active Coordinators");
        m.put("activeTabletServerCount", "Active TabletServers");
        m.put("bucketCount", "Bucket Count");
        m.put("partitionCount", "Partition Count");
        m.put("tableCount", "Table Count");
        m.put("offlineBucketCount", "Offline Buckets");
        m.put("kvSnapshotLeaseCount", "KV Snapshot Leases");
        m.put("leasedKvSnapshotCount", "Leased KV Snapshots");
        m.put("replicasToDeleteCount", "Replicas To Delete");
        m.put("remoteKvSize", "Remote KV Size");
        // Server general
        m.put("leaderCount", "Leader Replicas");
        m.put("replicaCount", "Total Replicas");
        m.put("atMinIsr", "At Min ISR");
        m.put("underMinIsr", "Under Min ISR");
        m.put("underReplicated", "Under Replicated");
        m.put("writerIdCount", "Active Writer IDs");
        m.put("localSize", "Local Storage Size");
        m.put("remoteLogSize", "Remote Log Size");
        m.put("kvSize", "KV Storage Size");
        m.put("logSize", "Log Storage Size");
        // Log bucket
        m.put("log.endOffset", "Log End Offset");
        m.put("log.numSegments", "Log Segments");
        m.put("remoteLog.endOffset", "Remote Log End Offset");
        m.put("remoteLog.numSegments", "Remote Log Segments");
        m.put("remoteLog.size", "Remote Log Size");
        // Remote log ops
        m.put("remoteLogCopyBytesPerSecond.count", "Remote Copy Bytes (cumul.)");
        m.put("remoteLogCopyBytesPerSecond.rate", "Remote Copy Bytes Rate");
        m.put("remoteLogCopyRequestsPerSecond.count", "Remote Copy Reqs (cumul.)");
        m.put("remoteLogCopyRequestsPerSecond.rate", "Remote Copy Req Rate");
        m.put("remoteLogCopyErrorPerSecond.count", "Remote Copy Errors (cumul.)");
        m.put("remoteLogCopyErrorPerSecond.rate", "Remote Copy Error Rate");
        m.put("remoteLogDeleteRequestsPerSecond.count", "Remote Delete Reqs (cumul.)");
        m.put("remoteLogDeleteRequestsPerSecond.rate", "Remote Delete Req Rate");
        m.put("remoteLogDeleteErrorPerSecond.count", "Remote Delete Errors (cumul.)");
        m.put("remoteLogDeleteErrorPerSecond.rate", "Remote Delete Error Rate");
        // Client RPC
        m.put("bytesInPerSecond_avg", "Bytes In Avg Rate");
        m.put("bytesInPerSecond_total", "Bytes In Total");
        m.put("bytesOutPerSecond_avg", "Bytes Out Avg Rate");
        m.put("bytesOutPerSecond_total", "Bytes Out Total");
        m.put("requestLatencyMs_avg", "Request Latency Avg (ms)");
        m.put("requestLatencyMs_max", "Request Latency Max (ms)");
        m.put("requestsInFlight_total", "Requests In Flight");
        m.put("requestsPerSecond_avg", "Requests Avg Rate");
        m.put("requestsPerSecond_total", "Requests Total");
        m.put("responsesPerSecond_avg", "Responses Avg Rate");
        m.put("responsesPerSecond_total", "Responses Total");
        // Lake
        m.put("lakeTableCount", "Lake Tables");
        m.put("pendingTablesCount", "Pending Tables");
        m.put("runningTablesCount", "Running Tables");
        return Collections.unmodifiableMap(m);
    }

    static String shortLabel(String key) {
        // Try matching suffix against label map (longest suffix match)
        for (Map.Entry<String, String> entry : LABEL_MAP.entrySet()) {
            if (key.endsWith(entry.getKey()) || key.endsWith("." + entry.getKey())) {
                return entry.getValue();
            }
        }
        // Fallback: take last 2 segments
        int lastDot = key.lastIndexOf('.');
        if (lastDot < 0) {
            return key;
        }
        int prevDot = key.lastIndexOf('.', lastDot - 1);
        if (prevDot < 0) {
            return key.substring(lastDot + 1);
        }
        return key.substring(prevDot + 1);
    }

    static boolean isAllZero(List<String> keys, List<ResourceSnapshot> snapshots) {
        for (String key : keys) {
            for (ResourceSnapshot snap : snapshots) {
                if (snap.serverMetrics == null) {
                    continue;
                }
                Object val = snap.serverMetrics.get(key);
                if (val instanceof Number && ((Number) val).doubleValue() != 0.0) {
                    return false;
                }
            }
        }
        return true;
    }

    /** Infers a display unit for a group of metric keys based on naming conventions. */
    static String inferUnit(List<String> keys) {
        for (String key : keys) {
            String lower = key.toLowerCase();
            // Time checks MUST come before memory checks because "timeMs" contains "mem"
            if (lower.contains("timems")
                    || lower.contains("latencyms")
                    || lower.contains("queuetimems")) {
                return "ms";
            }
            if (lower.contains("cpu.time")) {
                return "ms";
            }
            if (lower.contains("cpu.load")) {
                return "%";
            }
            if (lower.contains("latencymicros") || lower.contains("stallmicros")) {
                return "us";
            }
            if (lower.contains("memory")
                    || lower.contains("bytes")
                    || lower.contains("capacity")
                    || lower.contains("size")
                    || lower.contains("directmemory")
                    || (lower.contains("mem") && !lower.contains("timem"))) {
                return "MB";
            }
            if (lower.endsWith(".rate") || lower.contains("persecond.rate")) {
                return "/s";
            }
            if (lower.endsWith(".count") && lower.contains("persecond")) {
                return "count";
            }
        }
        return "";
    }

    /** Returns a divisor for converting raw metric values to display units. */
    static long unitDivisor(String key) {
        String lower = key.toLowerCase();
        // Bytes -> MB
        if (lower.contains("memoryu")
                || lower.contains("memoryused")
                || lower.contains("capacity")
                || lower.contains("directmemory")
                || lower.contains("heap.used")
                || lower.contains("heap.committed")
                || lower.contains("heap.max")
                || lower.contains("nonheap")
                || lower.contains("metaspace")) {
            return 1024L * 1024;
        }
        // CPU time in nanoseconds -> ms
        if (lower.contains("cpu.time")) {
            return 1_000_000L;
        }
        return 1;
    }

    /**
     * Collects and groups all server metric keys by logical category. Returns ordered groups and
     * populates groupMeta with [groupId, displayTitle] pairs.
     */
    static Map<String, List<String>> collectMetricGroups(
            List<ResourceSnapshot> snapshots, Map<String, String[]> groupMeta) {
        LinkedHashSet<String> allKeys = new LinkedHashSet<>();
        for (ResourceSnapshot snap : snapshots) {
            if (snap.serverMetrics != null) {
                allKeys.addAll(snap.serverMetrics.keySet());
            }
        }

        // Ordered group map
        Map<String, List<String>> groups = new LinkedHashMap<>();
        for (String key : allKeys) {
            String[] meta = classifyMetric(key);
            String groupId = meta[0];
            groupMeta.putIfAbsent(groupId, meta);
            groups.computeIfAbsent(groupId, k -> new ArrayList<>()).add(key);
        }

        // Sort groups by groupId for consistent ordering
        return new java.util.TreeMap<>(groups);
    }

    /**
     * Returns true if this key is a cumulative counter (ends with PerSecond.count) that is
     * redundant because a corresponding .rate metric always exists for PerSecond counters.
     */
    static boolean isRedundantCumul(String key) {
        return key.endsWith(".count") && key.contains("PerSecond");
    }

    static long findServerMetric(ResourceSnapshot snap, String keySuffix) {
        if (snap.serverMetrics == null) {
            return 0;
        }
        for (Map.Entry<String, Object> entry : snap.serverMetrics.entrySet()) {
            if (entry.getKey().endsWith(keySuffix) && entry.getValue() instanceof Number) {
                return ((Number) entry.getValue()).longValue();
            }
        }
        return 0;
    }
}
