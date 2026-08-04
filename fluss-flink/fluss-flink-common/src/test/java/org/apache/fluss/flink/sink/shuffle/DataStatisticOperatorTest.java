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

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.flink.sink.shuffle.StatisticsEvent.createStatisticsEvent;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DataStatisticsOperator}. */
public class DataStatisticOperatorTest {

    @Test
    void testProcessElement() throws Exception {
        DataStatisticsOperatorFactory<RowData> factory =
                new DataStatisticsOperatorFactory<>(
                        DATA1_ROW_TYPE,
                        Collections.singletonList("b"),
                        new RowDataSerializationSchema(false, false));
        List<StreamRecord<RowData>> inputRecords =
                Arrays.asList(
                        new StreamRecord<>(GenericRowData.of(1, StringData.fromString("a"))),
                        new StreamRecord<>(GenericRowData.of(2, StringData.fromString("a"))),
                        new StreamRecord<>(GenericRowData.of(3, StringData.fromString("b"))));

        List<StreamRecord<StatisticsOrRecord<RowData>>> expectedOutput = new ArrayList<>();

        try (DataStatisticOperatorTestHarness testHarness =
                new DataStatisticOperatorTestHarness(factory, 1, 1, 0)) {
            testHarness.open();
            assertThat(testHarness.getLocalStatistics()).isEmpty();

            //  process 3 records
            for (StreamRecord<RowData> record : inputRecords) {
                testHarness.processElement(record);
                expectedOutput.add(
                        new StreamRecord<>(StatisticsOrRecord.fromRecord(record.getValue())));
            }

            Map<String, Long> expectedLocalStatistic = new HashMap<>();
            expectedLocalStatistic.put("a", 10L);
            expectedLocalStatistic.put("b", 5L);
            assertThat(testHarness.getLocalStatistics()).isEqualTo(expectedLocalStatistic);
            testHarness.snapshot(0, 0L);
            assertThat(testHarness.getLocalStatistics()).isEmpty();
            assertThat(testHarness.getRecordOutput()).isEqualTo(expectedOutput);

            Map<String, Long> expectedGlobalStatistic = new HashMap<>();
            expectedGlobalStatistic.put("a", 10L);
            expectedGlobalStatistic.put("b", 5L);
            testHarness.handleOperatorEvent(
                    createStatisticsEvent(
                            0,
                            new DataStatistics(expectedGlobalStatistic),
                            new DataStatisticsSerializer()));
            expectedOutput.add(
                    new StreamRecord<>(
                            StatisticsOrRecord.fromStatistics(
                                    new DataStatistics(expectedGlobalStatistic))));
            assertThat(testHarness.getRecordOutput()).isEqualTo(expectedOutput);
        }
    }

    @Test
    void testProcessElementWithImplicitPartitionExpression() throws Exception {
        RowType rowType = implicitPartitionedRowType();
        DataStatisticsOperatorFactory<RowData> factory =
                new DataStatisticsOperatorFactory<>(
                        rowType,
                        Collections.singletonList("event_day"),
                        Collections.singletonList(
                                PartitionExpression.of(
                                        "event_day",
                                        DateTruncPartitionTransform.of(
                                                "event_time", AutoPartitionTimeUnit.DAY))),
                        new RowDataSerializationSchema(false, false));
        List<StreamRecord<RowData>> inputRecords =
                Arrays.asList(
                        new StreamRecord<>(rowData(1, LocalDateTime.of(2024, 3, 15, 10, 30), "a")),
                        new StreamRecord<>(rowData(2, LocalDateTime.of(2024, 3, 15, 23, 59), "b")),
                        new StreamRecord<>(rowData(3, LocalDateTime.of(2024, 3, 16, 0, 0), "c")));

        List<StreamRecord<StatisticsOrRecord<RowData>>> expectedOutput = new ArrayList<>();

        try (DataStatisticOperatorTestHarness testHarness =
                new DataStatisticOperatorTestHarness(factory, 1, 1, 0)) {
            testHarness.open();

            for (StreamRecord<RowData> record : inputRecords) {
                testHarness.processElement(record);
                expectedOutput.add(
                        new StreamRecord<>(StatisticsOrRecord.fromRecord(record.getValue())));
            }

            Map<String, Long> expectedLocalStatistic = new HashMap<>();
            expectedLocalStatistic.put("20240315", 26L);
            expectedLocalStatistic.put("20240316", 13L);
            assertThat(testHarness.getLocalStatistics()).isEqualTo(expectedLocalStatistic);
            assertThat(testHarness.getRecordOutput()).isEqualTo(expectedOutput);
        }
    }

    private static RowType implicitPartitionedRowType() {
        return RowType.of(
                new DataType[] {DataTypes.INT(), DataTypes.TIMESTAMP(), DataTypes.STRING()},
                new String[] {"id", "event_time", "payload"});
    }

    private static GenericRowData rowData(int id, LocalDateTime eventTime, String payload) {
        return GenericRowData.of(
                id, TimestampData.fromLocalDateTime(eventTime), StringData.fromString(payload));
    }

    static class DataStatisticOperatorTestHarness
            extends OneInputStreamOperatorTestHarness<RowData, StatisticsOrRecord<RowData>> {
        public DataStatisticOperatorTestHarness(
                DataStatisticsOperatorFactory<RowData> factory,
                int maxParallelism,
                int parallelism,
                int subtaskIndex)
                throws Exception {
            super(factory, maxParallelism, parallelism, subtaskIndex);
        }

        void handleOperatorEvent(OperatorEvent event) {
            ((DataStatisticsOperator<RowData>) operator).handleOperatorEvent(event);
        }

        Map<String, Long> getLocalStatistics() {
            return ((DataStatisticsOperator<RowData>) operator).getLocalStatistics().result();
        }
    }
}
