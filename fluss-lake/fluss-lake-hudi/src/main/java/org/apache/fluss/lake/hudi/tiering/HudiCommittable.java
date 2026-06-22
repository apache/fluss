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

import org.apache.hudi.client.WriteStatus;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** The committable aggregated from Hudi write results for one tiering round. */
public class HudiCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, List<WriteStatus>> writeStatuses;
    private final Map<String, List<WriteStatus>> compactionWriteStatuses;

    public HudiCommittable(
            Map<String, List<WriteStatus>> writeStatuses,
            @Nullable Map<String, List<WriteStatus>> compactionWriteStatuses) {
        this.writeStatuses = copyWriteStatuses(writeStatuses);
        this.compactionWriteStatuses = copyWriteStatuses(compactionWriteStatuses);
    }

    public Map<String, List<WriteStatus>> getWriteStatuses() {
        return writeStatuses;
    }

    public Map<String, List<WriteStatus>> getCompactionWriteStatuses() {
        return compactionWriteStatuses;
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Map<String, List<WriteStatus>> copyWriteStatuses(
            @Nullable Map<String, List<WriteStatus>> statusesByInstant) {
        if (statusesByInstant == null || statusesByInstant.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, List<WriteStatus>> copiedStatuses = new HashMap<>();
        for (Map.Entry<String, List<WriteStatus>> entry : statusesByInstant.entrySet()) {
            copiedStatuses.put(
                    entry.getKey(),
                    Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
        }
        return Collections.unmodifiableMap(copiedStatuses);
    }

    /** Builder for {@link HudiCommittable}. */
    public static class Builder {

        private final Map<String, List<WriteStatus>> writeStatuses = new HashMap<>();
        private final Map<String, List<WriteStatus>> compactionWriteStatuses = new HashMap<>();

        public Builder addWriteStatuses(Map<String, List<WriteStatus>> statusesByInstant) {
            addAll(writeStatuses, statusesByInstant);
            return this;
        }

        public Builder addCompactionWriteStatuses(
                @Nullable Map<String, List<WriteStatus>> statusesByInstant) {
            addAll(compactionWriteStatuses, statusesByInstant);
            return this;
        }

        public HudiCommittable build() {
            return new HudiCommittable(writeStatuses, compactionWriteStatuses);
        }

        private static void addAll(
                Map<String, List<WriteStatus>> target,
                @Nullable Map<String, List<WriteStatus>> source) {
            if (source == null || source.isEmpty()) {
                return;
            }
            for (Map.Entry<String, List<WriteStatus>> entry : source.entrySet()) {
                List<WriteStatus> statuses =
                        target.computeIfAbsent(entry.getKey(), ignored -> new ArrayList<>());
                statuses.addAll(entry.getValue());
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
        return Objects.equals(writeStatuses, that.writeStatuses)
                && Objects.equals(compactionWriteStatuses, that.compactionWriteStatuses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(writeStatuses, compactionWriteStatuses);
    }

    @Override
    public String toString() {
        return "HudiCommittable{"
                + "writeStatuses="
                + writeStatuses
                + ", compactionWriteStatuses="
                + compactionWriteStatuses
                + '}';
    }
}
