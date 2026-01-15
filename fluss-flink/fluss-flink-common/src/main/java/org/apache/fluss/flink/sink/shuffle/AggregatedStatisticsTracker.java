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

package org.apache.fluss.flink.sink.shuffle;

import org.apache.fluss.annotation.Internal;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * AggregatedStatisticsTracker tracks the statistics aggregation received from {@link
 * DataStatisticsOperator} subtasks for every checkpoint.
 *
 * <p>NOTE: This class is inspired from Iceberg project.
 */
@Internal
class AggregatedStatisticsTracker {
    private static final Logger LOG = LoggerFactory.getLogger(AggregatedStatisticsTracker.class);

    private final String operatorName;
    private final int parallelism;
    private final TypeSerializer<DataStatistics> statisticsSerializer;
    private final NavigableMap<Long, Aggregation> aggregationsPerCheckpoint;

    private long completedCheckpointId;
    private DataStatistics completedStatistics;

    AggregatedStatisticsTracker(String operatorName, int parallelism) {
        this.operatorName = operatorName;
        this.parallelism = parallelism;
        this.statisticsSerializer = new DataStatisticsSerializer();
        this.aggregationsPerCheckpoint = new TreeMap<>();
        this.completedCheckpointId = -1;
    }

    /**
     * Update the aggregated statistics for the given checkpoint id with statistics received from a
     * subtask.
     *
     * <p>This method collects statistics from different subtasks for a specific checkpoint and
     * tracks when all subtasks have reported their statistics. Once statistics from all subtasks
     * have been received for a checkpoint, the method returns the complete aggregated statistics.
     * Otherwise, it returns null to indicate that more statistics are expected or the statistic is
     * useless.
     */
    DataStatistics updateAndCheckCompletion(int subtask, StatisticsEvent event) {
        long checkpointId = event.getCheckpointId();
        LOG.debug(
                "Handling statistics event from subtask {} of operator {} for checkpoint {}",
                subtask,
                operatorName,
                checkpointId);

        if (completedStatistics != null && completedCheckpointId > checkpointId) {
            LOG.debug(
                    "Ignore stale statistics event from operator {} subtask {} for older checkpoint {}. "
                            + "Was expecting data statistics from checkpoint higher than {}",
                    operatorName,
                    subtask,
                    checkpointId,
                    completedCheckpointId);
            return null;
        }

        Aggregation aggregation =
                aggregationsPerCheckpoint.computeIfAbsent(
                        checkpointId, ignored -> new Aggregation(parallelism));
        DataStatistics dataStatistics =
                StatisticsUtil.deserializeDataStatistics(
                        event.getStatisticsBytes(), statisticsSerializer);
        if (!aggregation.merge(subtask, dataStatistics)) {
            LOG.debug(
                    "Ignore duplicate data statistics from operator {} subtask {} for checkpoint {}.",
                    operatorName,
                    subtask,
                    checkpointId);
        }

        if (aggregation.isComplete()) {
            this.completedStatistics = aggregation.completedStatistics();
            this.completedCheckpointId = checkpointId;
            // clean up aggregations up to the completed checkpoint id
            aggregationsPerCheckpoint.headMap(checkpointId, true).clear();
            return completedStatistics;
        }

        return null;
    }

    static class Aggregation {
        private static final Logger LOG = LoggerFactory.getLogger(Aggregation.class);
        private final int parallelism;
        private final Set<Integer> subtaskSet;
        private final Map<String, Long> partitionStatistics;

        Aggregation(int parallelism) {
            this.parallelism = parallelism;
            this.subtaskSet = new HashSet<>();
            this.partitionStatistics = new HashMap<>();
        }

        private boolean isComplete() {
            return subtaskSet.size() == parallelism;
        }

        /**
         * @return false if duplicate
         */
        private boolean merge(int subtask, DataStatistics taskStatistics) {
            if (subtaskSet.contains(subtask)) {
                return false;
            }

            subtaskSet.add(subtask);
            Map<String, Long> result = taskStatistics.result();
            result.forEach(
                    (partition, count) -> partitionStatistics.merge(partition, count, Long::sum));
            return true;
        }

        private DataStatistics completedStatistics() {
            return new DataStatistics(partitionStatistics);
        }
    }
}
