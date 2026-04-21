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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Immutable result of a single workload phase. */
public class PhaseResult {

    private final String phaseName;
    private final long totalOps;
    private final long totalBytes;
    private final long elapsedNanos;
    private final long p50Nanos;
    private final long p95Nanos;
    private final long p99Nanos;
    private final long p999Nanos;
    private final long p9999Nanos;
    private final long maxNanos;
    private final double opsPerSecond;
    private final double bytesPerSecond;
    private final boolean warmupOnly;
    private transient long[][] latencyHistogram;

    @JsonCreator
    public PhaseResult(
            @JsonProperty("phaseName") String phaseName,
            @JsonProperty("totalOps") long totalOps,
            @JsonProperty("totalBytes") long totalBytes,
            @JsonProperty("elapsedNanos") long elapsedNanos,
            @JsonProperty("p50Nanos") long p50Nanos,
            @JsonProperty("p95Nanos") long p95Nanos,
            @JsonProperty("p99Nanos") long p99Nanos,
            @JsonProperty("p999Nanos") long p999Nanos,
            @JsonProperty("p9999Nanos") long p9999Nanos,
            @JsonProperty("maxNanos") long maxNanos,
            @JsonProperty("opsPerSecond") double opsPerSecond,
            @JsonProperty("bytesPerSecond") double bytesPerSecond,
            @JsonProperty("warmupOnly") boolean warmupOnly) {
        this.phaseName = phaseName;
        this.totalOps = totalOps;
        this.totalBytes = totalBytes;
        this.elapsedNanos = elapsedNanos;
        this.p50Nanos = p50Nanos;
        this.p95Nanos = p95Nanos;
        this.p99Nanos = p99Nanos;
        this.p999Nanos = p999Nanos;
        this.p9999Nanos = p9999Nanos;
        this.maxNanos = maxNanos;
        this.opsPerSecond = opsPerSecond;
        this.bytesPerSecond = bytesPerSecond;
        this.warmupOnly = warmupOnly;
        this.latencyHistogram = null;
    }

    /**
     * Creates a {@link PhaseResult} by reading current stats from the given recorder and counter.
     */
    public static PhaseResult from(
            String name, LatencyRecorder latency, ThroughputCounter throughput, long elapsedNanos) {
        return from(name, latency, throughput, elapsedNanos, false);
    }

    /**
     * Creates a {@link PhaseResult} by reading current stats from the given recorder and counter.
     */
    public static PhaseResult from(
            String name,
            LatencyRecorder latency,
            ThroughputCounter throughput,
            long elapsedNanos,
            boolean warmupOnly) {
        PhaseResult result =
                new PhaseResult(
                        name,
                        throughput.totalOps(),
                        throughput.totalBytes(),
                        elapsedNanos,
                        latency.p50Nanos(),
                        latency.p95Nanos(),
                        latency.p99Nanos(),
                        latency.p999Nanos(),
                        latency.p9999Nanos(),
                        latency.maxNanos(),
                        throughput.opsPerSecond(elapsedNanos),
                        throughput.bytesPerSecond(elapsedNanos),
                        warmupOnly);
        result.latencyHistogram = latency.histogram(20);
        return result;
    }

    public long[][] latencyHistogram() {
        if (latencyHistogram == null) {
            return null;
        }
        long[][] copy = new long[latencyHistogram.length][];
        for (int i = 0; i < latencyHistogram.length; i++) {
            copy[i] = latencyHistogram[i].clone();
        }
        return copy;
    }

    @JsonProperty("phaseName")
    public String phaseName() {
        return phaseName;
    }

    @JsonProperty("totalOps")
    public long totalOps() {
        return totalOps;
    }

    @JsonProperty("totalBytes")
    public long totalBytes() {
        return totalBytes;
    }

    @JsonProperty("elapsedNanos")
    public long elapsedNanos() {
        return elapsedNanos;
    }

    @JsonProperty("p50Nanos")
    public long p50Nanos() {
        return p50Nanos;
    }

    @JsonProperty("p95Nanos")
    public long p95Nanos() {
        return p95Nanos;
    }

    @JsonProperty("p99Nanos")
    public long p99Nanos() {
        return p99Nanos;
    }

    @JsonProperty("p999Nanos")
    public long p999Nanos() {
        return p999Nanos;
    }

    @JsonProperty("p9999Nanos")
    public long p9999Nanos() {
        return p9999Nanos;
    }

    @JsonProperty("maxNanos")
    public long maxNanos() {
        return maxNanos;
    }

    @JsonProperty("opsPerSecond")
    public double opsPerSecond() {
        return opsPerSecond;
    }

    @JsonProperty("bytesPerSecond")
    public double bytesPerSecond() {
        return bytesPerSecond;
    }

    @JsonProperty("warmupOnly")
    public boolean warmupOnly() {
        return warmupOnly;
    }
}
