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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Pre-computed summary KPIs for a performance report. Immutable. */
public class ReportSummary {

    private final long peakClientRssBytes;
    private final long peakClientHeapBytes;
    private final long peakServerRssBytes;
    private final double bestOpsPerSecond;
    private final double bestBytesPerSecond;
    private final long totalOps;
    private final long totalBytes;
    private final long peakServerGcCount;
    private final long peakServerGcTimeMs;

    @JsonCreator
    public ReportSummary(
            @JsonProperty("peakClientRssBytes") long peakClientRssBytes,
            @JsonProperty("peakClientHeapBytes") long peakClientHeapBytes,
            @JsonProperty("peakServerRssBytes") long peakServerRssBytes,
            @JsonProperty("bestOpsPerSecond") double bestOpsPerSecond,
            @JsonProperty("bestBytesPerSecond") double bestBytesPerSecond,
            @JsonProperty("totalOps") long totalOps,
            @JsonProperty("totalBytes") long totalBytes,
            @JsonProperty("peakServerGcCount") long peakServerGcCount,
            @JsonProperty("peakServerGcTimeMs") long peakServerGcTimeMs) {
        this.peakClientRssBytes = peakClientRssBytes;
        this.peakClientHeapBytes = peakClientHeapBytes;
        this.peakServerRssBytes = peakServerRssBytes;
        this.bestOpsPerSecond = bestOpsPerSecond;
        this.bestBytesPerSecond = bestBytesPerSecond;
        this.totalOps = totalOps;
        this.totalBytes = totalBytes;
        this.peakServerGcCount = peakServerGcCount;
        this.peakServerGcTimeMs = peakServerGcTimeMs;
    }

    @JsonProperty("peakClientRssBytes")
    public long peakClientRssBytes() {
        return peakClientRssBytes;
    }

    @JsonProperty("peakClientHeapBytes")
    public long peakClientHeapBytes() {
        return peakClientHeapBytes;
    }

    @JsonProperty("peakServerRssBytes")
    public long peakServerRssBytes() {
        return peakServerRssBytes;
    }

    @JsonProperty("bestOpsPerSecond")
    public double bestOpsPerSecond() {
        return bestOpsPerSecond;
    }

    @JsonProperty("bestBytesPerSecond")
    public double bestBytesPerSecond() {
        return bestBytesPerSecond;
    }

    @JsonProperty("totalOps")
    public long totalOps() {
        return totalOps;
    }

    @JsonProperty("totalBytes")
    public long totalBytes() {
        return totalBytes;
    }

    @JsonProperty("peakServerGcCount")
    public long peakServerGcCount() {
        return peakServerGcCount;
    }

    @JsonProperty("peakServerGcTimeMs")
    public long peakServerGcTimeMs() {
        return peakServerGcTimeMs;
    }
}
