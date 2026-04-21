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

package org.apache.fluss.microbench.sampling;

import java.util.LinkedHashMap;
import java.util.Map;

/** Combined dual-process (client + server) resource snapshot. */
public class ResourceSnapshot {
    public long timestamp;

    // Client process (self)
    public ProcessSnapshot client;
    public long clientHeapUsed;
    public long clientHeapMax;
    public long clientNonHeapUsed;
    public long clientDirectMemoryUsed;
    public double clientJvmCpuLoad;
    public long clientGcCount;
    public long clientGcTimeMs;
    // Writer metrics
    public long clientBytesSendTotal;
    public long clientRecordsSendTotal;
    public double clientSendLatencyMs;
    public double clientBatchQueueTimeMs;
    public long clientRecordsRetryTotal;
    public double clientBytesPerBatchMean;
    public double clientRecordsPerBatchMean;
    // RPC metrics
    public double clientRpcRequestRate;
    public double clientRpcResponseRate;
    public double clientRpcBytesInRate;
    public double clientRpcBytesOutRate;
    public double clientRpcRequestLatencyAvg;
    public double clientRpcRequestLatencyMax;
    public double clientRpcInflight;

    // Disk I/O rates (bytes/sec), computed from consecutive sample deltas
    public long clientDiskReadBytesPerSec;
    public long clientDiskWriteBytesPerSec;
    public long serverDiskReadBytesPerSec;
    public long serverDiskWriteBytesPerSec;

    // Server process (child)
    public ProcessSnapshot server;
    public volatile long serverNmtHeap = -1;
    public volatile long serverNmtClass = -1;
    public volatile long serverNmtThreadStacks = -1;
    public volatile long serverNmtCodeCache = -1;
    public volatile long serverNmtGc = -1;
    public volatile long serverNmtInternal = -1;
    public volatile long serverRocksdbMemtable;
    public volatile long serverRocksdbBlockCache;
    public volatile long serverRocksdbTableReaders;
    public volatile long serverNettyDirectMemory;
    public volatile long serverNativeUntracked;

    /**
     * Full server metrics dump from FlussMetricSampler. Contains all Counter, Gauge, Meter, and
     * Histogram values keyed by their qualified metric name.
     */
    public Map<String, Object> serverMetrics = new LinkedHashMap<>();
}
