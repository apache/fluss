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

import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.assertj.core.data.Percentage;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.sink.shuffle.StatisticsOrRecord.fromRecord;
import static org.apache.fluss.flink.sink.shuffle.StatisticsOrRecord.fromStatistics;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link StatisticsOrRecordChannelComputer}. */
class StatisticsOrRecordChannelComputerTest {
    private static final FlussSerializationSchema<RowData> serializationSchema =
            new RowDataSerializationSchema(false, false);

    @BeforeAll
    static void init() throws Exception {
        serializationSchema.open(new SerializerInitContextImpl(DATA1_ROW_TYPE, false));
    }

    @Test
    void testEmptyPartitionKeys() {
        assertThatThrownBy(() -> channelComputer(false, 1, 1, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Partition keys cannot be empty.");
        assertThatThrownBy(() -> channelComputer(true, 1, 1, Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Partition keys cannot be empty.");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testShuffleWithoutStatistics(boolean hasBucketKeys) {
        int bucketNum = 3;
        int downstreamParallelism = 8;
        int targetParallelism = Math.min(bucketNum, downstreamParallelism);
        String partitionName = "partition 1";
        StatisticsOrRecordChannelComputer<RowData> channelComputer =
                channelComputer(hasBucketKeys, bucketNum, downstreamParallelism);
        Map<Integer, Double> subtaskAssignedCounts =
                runShuffle(Collections.singletonList(partitionName), channelComputer);
        assertThat(subtaskAssignedCounts.size()).isEqualTo(targetParallelism);
        // If no statistics, the rows of a partition should be distributed evenly to
        // min(bucketNum,downstreamParallelism) subtasks.
        for (Double percentage : subtaskAssignedCounts.values()) {
            assertThat(percentage).isCloseTo(1.0 / targetParallelism, Percentage.withPercentage(5));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testShuffleSingleTableWithSingleStatistics(boolean hasBucketKeys) {
        int bucketNum = 3;
        int downstreamParallelism = 8;
        Map<String, Long> partitionFrequency = new HashMap<>();
        partitionFrequency.put("partition 1", 10000L);
        StatisticsOrRecordChannelComputer<RowData> channelComputer =
                channelComputer(hasBucketKeys, bucketNum, downstreamParallelism);
        channelComputer.channel(fromStatistics(new DataStatistics(partitionFrequency)));
        Map<Integer, Double> subtaskAssignedCounts =
                runShuffle(Collections.singletonList("partition 1"), channelComputer);

        assertThat(subtaskAssignedCounts.size()).isEqualTo(downstreamParallelism);
        for (Double percentage : subtaskAssignedCounts.values()) {
            assertThat(percentage)
                    .isCloseTo(1.0 / downstreamParallelism, Percentage.withPercentage(5));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testShuffleSingleTableWithMultipleStatistics(boolean hasBucketKeys) {
        int bucketNum = 3;
        int downstreamParallelism = 8;
        Map<String, Long> partitionFrequency = new HashMap<>();
        partitionFrequency.put("partition 1", 10000L);
        partitionFrequency.put("partition 2", 30000L);
        StatisticsOrRecordChannelComputer<RowData> channelComputer =
                channelComputer(hasBucketKeys, bucketNum, downstreamParallelism);
        channelComputer.channel(fromStatistics(new DataStatistics(partitionFrequency)));
        Map<Integer, Double> subtaskAssignedCounts =
                runShuffle(Collections.singletonList("partition 1"), channelComputer);

        // partition 1 is only a quarter of the total rows, so the rows of partition 1 should be
        // distributed evenly to min(bucketNum,downstreamParallelism/4) subtasks.
        assertThat(subtaskAssignedCounts.size()).isEqualTo(downstreamParallelism / 4);
        for (Double percentage : subtaskAssignedCounts.values()) {
            assertThat(percentage)
                    .isCloseTo(4.0 / downstreamParallelism, Percentage.withPercentage(5));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testShuffleMultipleTablesWithMultipleStatistics(boolean hasBucketKeys) {
        int bucketNum = 3;
        int downstreamParallelism = 8;
        Map<String, Long> partitionFrequency = new HashMap<>();
        partitionFrequency.put("partition 1", 10000L);
        partitionFrequency.put("partition 2", 30000L);
        StatisticsOrRecordChannelComputer<RowData> channelComputer =
                channelComputer(hasBucketKeys, bucketNum, downstreamParallelism);
        channelComputer.channel(fromStatistics(new DataStatistics(partitionFrequency)));
        Map<Integer, Double> subtaskAssignedCounts =
                runShuffle(channelComputer, partitionFrequency);

        assertThat(subtaskAssignedCounts.size()).isEqualTo(downstreamParallelism);
        for (Double percentage : subtaskAssignedCounts.values()) {
            assertThat(percentage)
                    .isCloseTo(1.0 / downstreamParallelism, Percentage.withPercentage(5));
        }
    }

    Map<Integer, Double> runShuffle(
            List<String> partitions, StatisticsOrRecordChannelComputer<RowData> channelComputer) {
        return runShuffle(
                channelComputer, partitions.stream().collect(Collectors.toMap(p -> p, p -> 1L)));
    }

    Map<Integer, Double> runShuffle(
            StatisticsOrRecordChannelComputer<RowData> channelComputer,
            Map<String, Long> partitionFrequency) {
        int totalRowNum = 50000;
        Map<Integer, Double> subtaskAssignedCounts = new HashMap<>();
        long totalFrequency = partitionFrequency.values().stream().mapToLong(Long::longValue).sum();
        for (Map.Entry<String, Long> entry : partitionFrequency.entrySet()) {
            for (int i = 0;
                    i < Math.ceil(1.0 * entry.getValue() / totalFrequency * totalRowNum);
                    i++) {
                GenericRowData row = GenericRowData.of(i, StringData.fromString(entry.getKey()));
                subtaskAssignedCounts.merge(
                        channelComputer.channel(fromRecord(row)), 1.0 / totalRowNum, Double::sum);
            }
        }

        return subtaskAssignedCounts;
    }

    private static @NotNull StatisticsOrRecordChannelComputer<RowData> channelComputer(
            boolean hasBucketKeys, int bucketNum, int downstreamParallelism) {
        return channelComputer(
                hasBucketKeys, bucketNum, downstreamParallelism, Collections.singletonList("b"));
    }

    private static @NotNull StatisticsOrRecordChannelComputer<RowData> channelComputer(
            boolean hasBucketKeys,
            int bucketNum,
            int downstreamParallelism,
            List<String> partitionKeys) {
        StatisticsOrRecordChannelComputer<RowData> channelComputer =
                new StatisticsOrRecordChannelComputer<>(
                        DATA1_ROW_TYPE,
                        hasBucketKeys ? Collections.singletonList("a") : Collections.emptyList(),
                        partitionKeys,
                        bucketNum,
                        null,
                        serializationSchema);
        channelComputer.setup(downstreamParallelism);
        channelComputer.setRandom(new MockRandom());
        return channelComputer;
    }
}
