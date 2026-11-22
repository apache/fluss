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

package org.apache.fluss.lake.hudi.tiering;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** The committable aggregated from Hudi write results for one tiering round. */
public class HudiCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, HudiWriteStats> writeStats;
    private final Map<String, HudiWriteStats> compactionWriteStats;

    public HudiCommittable(
            Map<String, HudiWriteStats> writeStats,
            @Nullable Map<String, HudiWriteStats> compactionWriteStats) {
        this.writeStats = copyWriteStats(writeStats);
        this.compactionWriteStats = copyWriteStats(compactionWriteStats);
    }

    public Map<String, HudiWriteStats> getWriteStats() {
        return writeStats;
    }

    public Map<String, HudiWriteStats> getCompactionWriteStats() {
        return compactionWriteStats;
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Map<String, HudiWriteStats> copyWriteStats(
            @Nullable Map<String, HudiWriteStats> statsByInstant) {
        if (statsByInstant == null || statsByInstant.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new HashMap<>(statsByInstant));
    }

    /** Builder for {@link HudiCommittable}. */
    public static class Builder {

        private final Map<String, HudiWriteStats> writeStats = new HashMap<>();
        private final Map<String, HudiWriteStats> compactionWriteStats = new HashMap<>();

        public Builder addWriteStats(Map<String, HudiWriteStats> statsByInstant) {
            addAll(writeStats, statsByInstant);
            return this;
        }

        public Builder addCompactionWriteStats(
                @Nullable Map<String, HudiWriteStats> statsByInstant) {
            addAll(compactionWriteStats, statsByInstant);
            return this;
        }

        public HudiCommittable build() {
            return new HudiCommittable(writeStats, compactionWriteStats);
        }

        private static void addAll(
                Map<String, HudiWriteStats> target, @Nullable Map<String, HudiWriteStats> source) {
            if (source == null || source.isEmpty()) {
                return;
            }
            for (Map.Entry<String, HudiWriteStats> entry : source.entrySet()) {
                target.merge(entry.getKey(), entry.getValue(), HudiWriteStats::merge);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HudiCommittable)) {
            return false;
        }
        HudiCommittable that = (HudiCommittable) o;
        return Objects.equals(writeStats, that.writeStats)
                && Objects.equals(compactionWriteStats, that.compactionWriteStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(writeStats, compactionWriteStats);
    }

    @Override
    public String toString() {
        return "HudiCommittable{"
                + "writeStats="
                + writeStats
                + ", compactionWriteStats="
                + compactionWriteStats
                + '}';
    }
}
