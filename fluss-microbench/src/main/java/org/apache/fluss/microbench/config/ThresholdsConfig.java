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

import javax.annotation.Nullable;

/**
 * Pass/fail threshold configuration for regression detection.
 *
 * <p>YAML structure matches nested form used by presets:
 *
 * <pre>{@code
 * thresholds:
 *   write-tps:
 *     min: 100000
 *   rss-peak-mb:
 *     max: 1500
 *   heap-mb:
 *     max: 512
 *   p99-ms:
 *     max: 10
 * }</pre>
 */
public class ThresholdsConfig {

    private ThresholdBound writeTps;
    private ThresholdBound rssPeakMb;
    private ThresholdBound heapMb;
    private ThresholdBound p99Ms;

    @Nullable
    public ThresholdBound writeTps() {
        return writeTps;
    }

    public void setWriteTps(ThresholdBound writeTps) {
        this.writeTps = writeTps;
    }

    @Nullable
    public ThresholdBound rssPeakMb() {
        return rssPeakMb;
    }

    public void setRssPeakMb(ThresholdBound rssPeakMb) {
        this.rssPeakMb = rssPeakMb;
    }

    @Nullable
    public ThresholdBound heapMb() {
        return heapMb;
    }

    public void setHeapMb(ThresholdBound heapMb) {
        this.heapMb = heapMb;
    }

    @Nullable
    public ThresholdBound p99Ms() {
        return p99Ms;
    }

    public void setP99Ms(ThresholdBound p99Ms) {
        this.p99Ms = p99Ms;
    }

    /** A min/max bound for a single threshold metric. */
    public static class ThresholdBound {
        private Long min;
        private Long max;

        @Nullable
        public Long min() {
            return min;
        }

        public void setMin(Long min) {
            this.min = min;
        }

        @Nullable
        public Long max() {
            return max;
        }

        public void setMax(Long max) {
            this.max = max;
        }
    }
}
