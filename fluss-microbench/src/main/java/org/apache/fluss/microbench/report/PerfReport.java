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

import org.apache.fluss.microbench.config.ScenarioConfig;
import org.apache.fluss.microbench.config.WorkloadPhaseConfig;
import org.apache.fluss.microbench.sampling.ResourceSnapshot;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Top-level performance report aggregating phase results, resource snapshots, and environment. */
public class PerfReport {

    private final String scenarioName;
    private final String configSnapshot;
    private final List<PhaseResult> phaseResults;
    private final List<ResourceSnapshot> snapshots;
    private final EnvironmentSnapshot environment;
    private final String status;
    @Nullable private final String error;
    @Nullable private final String description;
    @Nullable private final String mergeEngine;
    @Nullable private final Integer buckets;
    @Nullable private final String primaryKey;
    @Nullable private final String logFormat;
    @Nullable private final String kvFormat;
    @Nullable private final Integer tabletServers;
    @Nullable private final Long dataSeed;
    @Nullable private final List<WorkloadPhaseSummary> workloadPhases;
    @Nullable private final ReportSummary summary;

    @JsonCreator
    private PerfReport(
            @JsonProperty("scenarioName") String scenarioName,
            @JsonProperty("configSnapshot") String configSnapshot,
            @JsonProperty("phaseResults") List<PhaseResult> phaseResults,
            @JsonProperty("snapshots") List<ResourceSnapshot> snapshots,
            @JsonProperty("environment") EnvironmentSnapshot environment,
            @JsonProperty("status") String status,
            @JsonProperty("error") @Nullable String error,
            @JsonProperty("description") @Nullable String description,
            @JsonProperty("mergeEngine") @Nullable String mergeEngine,
            @JsonProperty("buckets") @Nullable Integer buckets,
            @JsonProperty("primaryKey") @Nullable String primaryKey,
            @JsonProperty("logFormat") @Nullable String logFormat,
            @JsonProperty("kvFormat") @Nullable String kvFormat,
            @JsonProperty("tabletServers") @Nullable Integer tabletServers,
            @JsonProperty("dataSeed") @Nullable Long dataSeed,
            @JsonProperty("workloadPhases") @Nullable List<WorkloadPhaseSummary> workloadPhases,
            @JsonProperty("summary") @Nullable ReportSummary summary) {
        this.scenarioName = scenarioName;
        this.configSnapshot = configSnapshot;
        this.phaseResults = Collections.unmodifiableList(new ArrayList<>(phaseResults));
        this.snapshots = Collections.unmodifiableList(new ArrayList<>(snapshots));
        this.environment = environment;
        this.status = status;
        this.error = error;
        this.description = description;
        this.mergeEngine = mergeEngine;
        this.buckets = buckets;
        this.primaryKey = primaryKey;
        this.logFormat = logFormat;
        this.kvFormat = kvFormat;
        this.tabletServers = tabletServers;
        this.dataSeed = dataSeed;
        this.workloadPhases = workloadPhases;
        this.summary = summary;
    }

    private PerfReport(Builder b) {
        this.scenarioName = b.scenarioName;
        this.configSnapshot = b.configSnapshot;
        this.phaseResults = Collections.unmodifiableList(new ArrayList<>(b.phaseResults));
        this.snapshots = Collections.unmodifiableList(new ArrayList<>(b.snapshots));
        this.environment = b.environment;
        this.status = b.status;
        this.error = b.error;
        this.description = b.description;
        this.mergeEngine = b.mergeEngine;
        this.buckets = b.buckets;
        this.primaryKey = b.primaryKey;
        this.logFormat = b.logFormat;
        this.kvFormat = b.kvFormat;
        this.tabletServers = b.tabletServers;
        this.dataSeed = b.dataSeed;
        this.workloadPhases = b.workloadPhases;
        this.summary = b.summary;
    }

    /** Returns a new builder for constructing {@link PerfReport} instances. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builds a report from a completed (or partially completed) benchmark run. */
    public static PerfReport build(
            ScenarioConfig config,
            String yamlSnapshot,
            List<PhaseResult> phases,
            List<ResourceSnapshot> snapshots,
            EnvironmentSnapshot env,
            String status,
            @Nullable String error) {
        return builderFromConfig(config)
                .configSnapshot(yamlSnapshot)
                .phaseResults(phases)
                .snapshots(snapshots)
                .environment(env)
                .status(status)
                .error(error)
                .summary(computeSummary(phases, snapshots))
                .build();
    }

    /** Extracts common structured fields from a {@link ScenarioConfig} into a builder. */
    private static Builder builderFromConfig(ScenarioConfig config) {
        Builder b = builder();
        b.scenarioName(
                config.meta() != null && config.meta().name() != null
                        ? config.meta().name()
                        : "unnamed");
        b.description(config.meta() != null ? config.meta().description() : null);
        b.mergeEngine(config.table() != null ? config.table().mergeEngine() : null);
        b.buckets(config.table() != null ? config.table().buckets() : null);
        b.primaryKey(
                config.table() != null && config.table().primaryKey() != null
                        ? String.join(", ", config.table().primaryKey())
                        : null);
        b.logFormat(config.table() != null ? config.table().logFormat() : null);
        b.kvFormat(config.table() != null ? config.table().kvFormat() : null);
        b.tabletServers(config.cluster() != null ? config.cluster().tabletServers() : null);
        b.dataSeed(config.data() != null ? config.data().seed() : null);

        List<WorkloadPhaseSummary> workloadPhases = new ArrayList<>();
        if (config.workload() != null) {
            for (WorkloadPhaseConfig wpc : config.workload()) {
                String keyRange =
                        wpc.keyRange() != null
                                ? wpc.keyRange().get(0) + ".." + wpc.keyRange().get(1)
                                : null;
                workloadPhases.add(
                        new WorkloadPhaseSummary(
                                wpc.phase(),
                                wpc.threads(),
                                wpc.records(),
                                wpc.duration(),
                                wpc.warmup(),
                                wpc.rateLimit(),
                                keyRange));
            }
        }
        b.workloadPhases(workloadPhases);
        return b;
    }

    private static ReportSummary computeSummary(
            List<PhaseResult> phases, List<ResourceSnapshot> snapshots) {
        long peakClientRss = 0, peakClientHeap = 0, peakServerRss = 0;
        for (ResourceSnapshot s : snapshots) {
            if (s.client != null && s.client.rssBytes > peakClientRss) {
                peakClientRss = s.client.rssBytes;
            }
            if (s.clientHeapUsed > peakClientHeap) {
                peakClientHeap = s.clientHeapUsed;
            }
            if (s.server != null && s.server.rssBytes > peakServerRss) {
                peakServerRss = s.server.rssBytes;
            }
        }
        double bestOps = 0, bestBytes = 0;
        long totalOps = 0, totalBytes = 0;
        for (PhaseResult p : phases) {
            if (!p.warmupOnly()) {
                if (p.opsPerSecond() > bestOps) {
                    bestOps = p.opsPerSecond();
                }
                if (p.bytesPerSecond() > bestBytes) {
                    bestBytes = p.bytesPerSecond();
                }
                totalOps += p.totalOps();
                totalBytes += p.totalBytes();
            }
        }
        return new ReportSummary(
                peakClientRss,
                peakClientHeap,
                peakServerRss,
                bestOps,
                bestBytes,
                totalOps,
                totalBytes,
                0,
                0);
    }

    @JsonProperty("scenarioName")
    public String scenarioName() {
        return scenarioName;
    }

    @JsonProperty("configSnapshot")
    public String configSnapshot() {
        return configSnapshot;
    }

    @JsonProperty("phaseResults")
    public List<PhaseResult> phaseResults() {
        return phaseResults;
    }

    @JsonProperty("snapshots")
    public List<ResourceSnapshot> snapshots() {
        return snapshots;
    }

    @JsonProperty("environment")
    public EnvironmentSnapshot environment() {
        return environment;
    }

    @JsonProperty("status")
    public String status() {
        return status;
    }

    @JsonProperty("error")
    @Nullable
    public String error() {
        return error;
    }

    @JsonProperty("description")
    @Nullable
    public String description() {
        return description;
    }

    @JsonProperty("mergeEngine")
    @Nullable
    public String mergeEngine() {
        return mergeEngine;
    }

    @JsonProperty("buckets")
    @Nullable
    public Integer buckets() {
        return buckets;
    }

    @JsonProperty("primaryKey")
    @Nullable
    public String primaryKey() {
        return primaryKey;
    }

    @JsonProperty("logFormat")
    @Nullable
    public String logFormat() {
        return logFormat;
    }

    @JsonProperty("kvFormat")
    @Nullable
    public String kvFormat() {
        return kvFormat;
    }

    @JsonProperty("tabletServers")
    @Nullable
    public Integer tabletServers() {
        return tabletServers;
    }

    @JsonProperty("dataSeed")
    @Nullable
    public Long dataSeed() {
        return dataSeed;
    }

    @JsonProperty("workloadPhases")
    @Nullable
    public List<WorkloadPhaseSummary> workloadPhases() {
        return workloadPhases;
    }

    @JsonProperty("summary")
    @Nullable
    public ReportSummary summary() {
        return summary;
    }

    /** Lightweight summary of a workload phase config for report rendering. */
    public static class WorkloadPhaseSummary {
        @JsonProperty("phase")
        public final String phase;

        @JsonProperty("threads")
        public final Integer threads;

        @JsonProperty("records")
        public final Long records;

        @JsonProperty("duration")
        public final String duration;

        @JsonProperty("warmup")
        public final String warmup;

        @JsonProperty("rateLimit")
        public final Long rateLimit;

        @JsonProperty("keyRange")
        public final String keyRange;

        @JsonCreator
        public WorkloadPhaseSummary(
                @JsonProperty("phase") String phase,
                @JsonProperty("threads") Integer threads,
                @JsonProperty("records") Long records,
                @JsonProperty("duration") String duration,
                @JsonProperty("warmup") String warmup,
                @JsonProperty("rateLimit") Long rateLimit,
                @JsonProperty("keyRange") String keyRange) {
            this.phase = phase;
            this.threads = threads;
            this.records = records;
            this.duration = duration;
            this.warmup = warmup;
            this.rateLimit = rateLimit;
            this.keyRange = keyRange;
        }
    }

    /** Mutable builder for constructing {@link PerfReport} instances. */
    public static class Builder {
        private String scenarioName = "unnamed";
        private String configSnapshot = "";
        private List<PhaseResult> phaseResults = Collections.emptyList();
        private List<ResourceSnapshot> snapshots = Collections.emptyList();
        private EnvironmentSnapshot environment;
        private String status = "complete";
        @Nullable private String error;
        @Nullable private String description;
        @Nullable private String mergeEngine;
        @Nullable private Integer buckets;
        @Nullable private String primaryKey;
        @Nullable private String logFormat;
        @Nullable private String kvFormat;
        @Nullable private Integer tabletServers;
        @Nullable private Long dataSeed;
        @Nullable private List<WorkloadPhaseSummary> workloadPhases;
        @Nullable private ReportSummary summary;

        private Builder() {}

        public Builder scenarioName(String v) {
            this.scenarioName = v;
            return this;
        }

        public Builder configSnapshot(String v) {
            this.configSnapshot = v;
            return this;
        }

        public Builder phaseResults(List<PhaseResult> v) {
            this.phaseResults = v;
            return this;
        }

        public Builder snapshots(List<ResourceSnapshot> v) {
            this.snapshots = v;
            return this;
        }

        public Builder environment(EnvironmentSnapshot v) {
            this.environment = v;
            return this;
        }

        public Builder status(String v) {
            this.status = v;
            return this;
        }

        public Builder error(@Nullable String v) {
            this.error = v;
            return this;
        }

        public Builder description(@Nullable String v) {
            this.description = v;
            return this;
        }

        public Builder mergeEngine(@Nullable String v) {
            this.mergeEngine = v;
            return this;
        }

        public Builder buckets(@Nullable Integer v) {
            this.buckets = v;
            return this;
        }

        public Builder primaryKey(@Nullable String v) {
            this.primaryKey = v;
            return this;
        }

        public Builder logFormat(@Nullable String v) {
            this.logFormat = v;
            return this;
        }

        public Builder kvFormat(@Nullable String v) {
            this.kvFormat = v;
            return this;
        }

        public Builder tabletServers(@Nullable Integer v) {
            this.tabletServers = v;
            return this;
        }

        public Builder dataSeed(@Nullable Long v) {
            this.dataSeed = v;
            return this;
        }

        public Builder workloadPhases(@Nullable List<WorkloadPhaseSummary> v) {
            this.workloadPhases = v;
            return this;
        }

        public Builder summary(@Nullable ReportSummary v) {
            this.summary = v;
            return this;
        }

        public PerfReport build() {
            if (summary == null && phaseResults != null && snapshots != null) {
                summary = computeSummary(phaseResults, snapshots);
            }
            return new PerfReport(this);
        }
    }
}
