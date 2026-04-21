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

package org.apache.fluss.microbench.config;

import java.util.List;
import java.util.Map;

/** A single workload phase (write, read, mixed, etc.). */
public class WorkloadPhaseConfig {

    private String phase;
    private Long records;
    private String duration;
    private Integer threads;
    private String warmup;
    private Long rateLimit;
    private Map<String, Integer> mix;
    private List<Long> keyRange;
    private Integer keyPrefixLength;
    private String fromOffset;
    private Integer maxRetries;
    private Map<String, String> filter;

    public String phase() {
        return phase;
    }

    public void setPhase(String phase) {
        this.phase = phase;
    }

    public Long records() {
        return records;
    }

    public void setRecords(Long records) {
        this.records = records;
    }

    public String duration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public Integer threads() {
        return threads;
    }

    public void setThreads(Integer threads) {
        this.threads = threads;
    }

    public String warmup() {
        return warmup;
    }

    public void setWarmup(String warmup) {
        this.warmup = warmup;
    }

    public Long rateLimit() {
        return rateLimit;
    }

    public void setRateLimit(Long rateLimit) {
        this.rateLimit = rateLimit;
    }

    public Map<String, Integer> mix() {
        return mix;
    }

    public void setMix(Map<String, Integer> mix) {
        this.mix = mix;
    }

    public List<Long> keyRange() {
        return keyRange;
    }

    public void setKeyRange(List<Long> keyRange) {
        this.keyRange = keyRange;
    }

    public Integer keyPrefixLength() {
        return keyPrefixLength;
    }

    public void setKeyPrefixLength(Integer keyPrefixLength) {
        this.keyPrefixLength = keyPrefixLength;
    }

    public String fromOffset() {
        return fromOffset;
    }

    public void setFromOffset(String fromOffset) {
        this.fromOffset = fromOffset;
    }

    /** Max retry attempts for this phase. Default is 0 (no retry). */
    public Integer maxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(Integer maxRetries) {
        this.maxRetries = maxRetries;
    }

    /**
     * Filter expression for scan phases. Keys: "column", "op", "value".
     *
     * <p>Supported ops: eq, neq, gt, gte, lt, lte.
     */
    public Map<String, String> filter() {
        return filter;
    }

    public void setFilter(Map<String, String> filter) {
        this.filter = filter;
    }
}
