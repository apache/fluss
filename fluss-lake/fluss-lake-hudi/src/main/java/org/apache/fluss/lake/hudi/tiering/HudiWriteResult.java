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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Write result produced by the Hudi lake writer and consumed by a future Hudi committer. */
public class HudiWriteResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, HudiWriteStats> writeStats;
    private final Map<String, HudiWriteStats> compactionWriteStats;

    public HudiWriteResult(
            Map<String, HudiWriteStats> writeStats,
            @Nullable Map<String, HudiWriteStats> compactionWriteStats) {
        this.writeStats = copyWriteStats(writeStats);
        this.compactionWriteStats = copyWriteStats(compactionWriteStats);
    }

    public static HudiWriteResult fromWriteStatuses(
            Map<String, List<WriteStatus>> writeStatuses,
            @Nullable Map<String, List<WriteStatus>> compactionWriteStatuses) {
        return new HudiWriteResult(
                toWriteStats(writeStatuses), toWriteStats(compactionWriteStatuses));
    }

    public Map<String, HudiWriteStats> getWriteStats() {
        return writeStats;
    }

    public Map<String, HudiWriteStats> getCompactionWriteStats() {
        return compactionWriteStats;
    }

    private static Map<String, HudiWriteStats> copyWriteStats(
            @Nullable Map<String, HudiWriteStats> statsByInstant) {
        if (statsByInstant == null || statsByInstant.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new HashMap<>(statsByInstant));
    }

    private static Map<String, HudiWriteStats> toWriteStats(
            @Nullable Map<String, List<WriteStatus>> statusesByInstant) {
        if (statusesByInstant == null || statusesByInstant.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, HudiWriteStats> statsByInstant = new HashMap<>();
        for (Map.Entry<String, List<WriteStatus>> entry : statusesByInstant.entrySet()) {
            statsByInstant.put(entry.getKey(), HudiWriteStats.fromWriteStatuses(entry.getValue()));
        }
        return statsByInstant;
    }
}
