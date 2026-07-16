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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.lake.committer.PartitionDoneCandidate;
import org.apache.fluss.lake.committer.PartitionDoneHandler;
import org.apache.fluss.metadata.ResolvedPartitionSpec;

import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Paimon implementation of {@link PartitionDoneHandler}.
 *
 * <p>It reuses Paimon's native {@link PartitionMarkDoneAction} (e.g. success-file) to perform the
 * actual mark-done, and follows Paimon's mark-done judgement:
 *
 * <pre>
 * done  &lt;=&gt;  currentTime - MAX(lastUpdateTime, partitionEndTime) &gt; idleTime
 * </pre>
 *
 * <p>The {@code partitionEndTime} is computed differently for auto-partitioned and
 * non-auto-partitioned tables:
 *
 * <ul>
 *   <li>Auto-partitioned tables: the partition value uses Fluss auto-partition format (e.g. {@code
 *       yyyyMMdd} for DAY). We parse it by the {@link AutoPartitionTimeUnit} and add exactly one
 *       calendar unit to get the (exact) partition end time.
 *   <li>Non-auto-partitioned tables: we use Paimon's {@link PartitionTimeExtractor} (configured via
 *       timestamp-pattern / timestamp-formatter) to get the partition time, used directly in the
 *       idle judgement.
 * </ul>
 *
 * <p>The action is idempotent (Paimon success-file keeps creationTime and only updates
 * modificationTime), so repeated execution is safe.
 */
class PaimonPartitionDoneHandler implements PartitionDoneHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonPartitionDoneHandler.class);

    private final List<PartitionMarkDoneAction> actions;
    private final List<String> partitionKeys;
    private final long idleTimeMs;
    private final ZoneId zoneId;

    // auto-partition mode: non-null time unit, timeExtractor unused
    @Nullable private final AutoPartitionTimeUnit autoPartitionTimeUnit;

    // non-auto-partition mode: non-null extractor
    @Nullable private final PartitionTimeExtractor timeExtractor;

    PaimonPartitionDoneHandler(
            List<PartitionMarkDoneAction> actions,
            List<String> partitionKeys,
            long idleTimeMs,
            ZoneId zoneId,
            @Nullable AutoPartitionTimeUnit autoPartitionTimeUnit,
            @Nullable PartitionTimeExtractor timeExtractor) {
        this.actions = actions;
        this.partitionKeys = partitionKeys;
        this.idleTimeMs = idleTimeMs;
        this.zoneId = zoneId;
        this.autoPartitionTimeUnit = autoPartitionTimeUnit;
        this.timeExtractor = timeExtractor;
    }

    @Override
    public List<String> markDoneIfReady(List<PartitionDoneCandidate> candidates, long currentTime)
            throws Exception {
        List<String> done = new ArrayList<>();
        for (PartitionDoneCandidate state : candidates) {
            ResolvedPartitionSpec spec =
                    ResolvedPartitionSpec.fromPartitionName(partitionKeys, state.partitionName());
            long partitionEndTime = partitionEndTime(spec);
            long lastUpdateTime = state.lastUpdateTime();

            // Cannot judge if both the last update time and the partition end time are unavailable.
            if (lastUpdateTime <= 0 && partitionEndTime <= 0) {
                LOG.warn(
                        "Skip mark-done for partition {}: both lastUpdateTime and partitionEndTime "
                                + "are unavailable.",
                        state.partitionName());
                continue;
            }

            long threshold = Math.max(lastUpdateTime, partitionEndTime);
            if (currentTime - threshold > idleTimeMs) {
                String paimonPartition = spec.getPartitionQualifiedName();
                for (PartitionMarkDoneAction action : actions) {
                    action.markDone(paimonPartition);
                }
                done.add(state.partitionName());
            }
        }
        return done;
    }

    /**
     * Computes the partition end time in epoch millis. Returns 0 when the time cannot be extracted,
     * which degrades the judgement to a pure idle check on {@code lastUpdateTime}.
     */
    private long partitionEndTime(ResolvedPartitionSpec spec) {
        try {
            if (autoPartitionTimeUnit != null) {
                return autoPartitionEndTimeMillis(spec.getPartitionValues().get(0));
            } else {
                LocalDateTime start =
                        timeExtractor.extract(spec.getPartitionKeys(), spec.getPartitionValues());
                return start.atZone(zoneId).toInstant().toEpochMilli();
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to extract partition time for partition {}, degrade to pure idle "
                            + "judgement.",
                    spec.getPartitionQualifiedName(),
                    e);
            return 0;
        }
    }

    /**
     * Computes the exact partition end time for an auto-partitioned table by parsing the Fluss
     * auto-partition value and adding exactly one calendar unit.
     */
    private long autoPartitionEndTimeMillis(String value) {
        LocalDateTime end;
        switch (autoPartitionTimeUnit) {
            case YEAR:
                {
                    // yyyy
                    int year = Integer.parseInt(value);
                    end = LocalDateTime.of(year, 1, 1, 0, 0).plusYears(1);
                    break;
                }
            case QUARTER:
                {
                    // yyyyQ
                    int year = Integer.parseInt(value.substring(0, 4));
                    int quarter = Integer.parseInt(value.substring(4));
                    end = LocalDateTime.of(year, (quarter - 1) * 3 + 1, 1, 0, 0).plusMonths(3);
                    break;
                }
            case MONTH:
                {
                    // yyyyMM
                    int year = Integer.parseInt(value.substring(0, 4));
                    int month = Integer.parseInt(value.substring(4, 6));
                    end = LocalDateTime.of(year, month, 1, 0, 0).plusMonths(1);
                    break;
                }
            case DAY:
                {
                    // yyyyMMdd
                    int year = Integer.parseInt(value.substring(0, 4));
                    int month = Integer.parseInt(value.substring(4, 6));
                    int day = Integer.parseInt(value.substring(6, 8));
                    end = LocalDateTime.of(year, month, day, 0, 0).plusDays(1);
                    break;
                }
            case HOUR:
                {
                    // yyyyMMddHH
                    int year = Integer.parseInt(value.substring(0, 4));
                    int month = Integer.parseInt(value.substring(4, 6));
                    int day = Integer.parseInt(value.substring(6, 8));
                    int hour = Integer.parseInt(value.substring(8, 10));
                    end = LocalDateTime.of(year, month, day, hour, 0).plusHours(1);
                    break;
                }
            default:
                throw new IllegalArgumentException(
                        "Unsupported auto partition time unit: " + autoPartitionTimeUnit);
        }
        return end.atZone(zoneId).toInstant().toEpochMilli();
    }

    @Override
    public void close() throws Exception {
        for (PartitionMarkDoneAction action : actions) {
            action.close();
        }
    }
}
