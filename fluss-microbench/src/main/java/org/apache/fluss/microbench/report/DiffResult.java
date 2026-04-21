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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/** Holds the result of comparing two {@link PerfReport} instances. */
public class DiffResult {

    private final List<MetricComparison> comparisons;

    public DiffResult(List<MetricComparison> comparisons) {
        this.comparisons = Collections.unmodifiableList(comparisons);
    }

    @JsonProperty("comparisons")
    public List<MetricComparison> comparisons() {
        return comparisons;
    }

    /** A single metric comparison between baseline and target. */
    public static class MetricComparison {

        private final String name;
        private final double baselineValue;
        private final double targetValue;
        private final double changePct;
        private final MetricDirection direction;
        private final String verdict;

        public MetricComparison(
                String name,
                double baselineValue,
                double targetValue,
                double changePct,
                MetricDirection direction,
                String verdict) {
            this.name = name;
            this.baselineValue = baselineValue;
            this.targetValue = targetValue;
            this.changePct = changePct;
            this.direction = direction;
            this.verdict = verdict;
        }

        @JsonProperty("name")
        public String name() {
            return name;
        }

        @JsonProperty("baseline_value")
        public double baselineValue() {
            return baselineValue;
        }

        @JsonProperty("target_value")
        public double targetValue() {
            return targetValue;
        }

        @JsonProperty("change_pct")
        public double changePct() {
            return changePct;
        }

        @JsonProperty("direction")
        public MetricDirection direction() {
            return direction;
        }

        @JsonProperty("verdict")
        public String verdict() {
            return verdict;
        }
    }
}
