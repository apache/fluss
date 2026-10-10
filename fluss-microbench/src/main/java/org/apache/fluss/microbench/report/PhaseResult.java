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

import org.apache.fluss.microbench.stats.LatencyRecorder;
import org.apache.fluss.microbench.stats.ThroughputCounter;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Throughput and latency measurements for one workload phase. */
public final class PhaseResult {

    @JsonProperty private final String phaseName;
    @JsonProperty private final long totalOps;
    @JsonProperty private final long totalBytes;
    @JsonProperty private final long elapsedNanos;
    @JsonProperty private final long p50Nanos;
    @JsonProperty private final long p95Nanos;
    @JsonProperty private final long p99Nanos;
    @JsonProperty private final long maxNanos;
    @JsonProperty private final double opsPerSecond;
    @JsonProperty private final double bytesPerSecond;

    private PhaseResult(
            String phaseName,
            ThroughputCounter throughput,
            LatencyRecorder latency,
            long elapsedNanos) {
        this.phaseName = phaseName;
        this.totalOps = throughput.totalOps();
        this.totalBytes = throughput.totalBytes();
        this.elapsedNanos = elapsedNanos;
        this.p50Nanos = latency.p50Nanos();
        this.p95Nanos = latency.p95Nanos();
        this.p99Nanos = latency.p99Nanos();
        this.maxNanos = latency.maxNanos();
        this.opsPerSecond = throughput.opsPerSecond(elapsedNanos);
        this.bytesPerSecond = throughput.bytesPerSecond(elapsedNanos);
    }

    public static PhaseResult from(
            String name, LatencyRecorder latency, ThroughputCounter throughput, long elapsedNanos) {
        return new PhaseResult(name, throughput, latency, elapsedNanos);
    }
}
