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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link FlinkRecordsWithSplitIds}. */
class FlinkRecordsWithSplitIdsTest {

    @Test
    void testReportsEventTimeLagForBoundedRecords() {
        RecordingFlinkSourceReaderMetrics metrics = new RecordingFlinkSourceReaderMetrics();
        long timestamp = System.currentTimeMillis() - 10000L;
        RecordAndPos record =
                new RecordAndPos(
                        new ScanRecord(-1L, timestamp, ChangeType.INSERT, row(1, "a")), 1L);
        FlinkRecordsWithSplitIds records =
                new FlinkRecordsWithSplitIds(
                        "split",
                        new TableBucket(1L, 0),
                        CloseableIterator.wrap(Collections.singletonList(record).iterator()),
                        metrics);

        assertThat(records.nextSplit()).isEqualTo("split");
        assertThat(records.nextRecordFromSplit()).isEqualTo(record);
        assertThat(metrics.reportedTableBucket).isEqualTo(new TableBucket(1L, 0));
        assertThat(metrics.reportedTimestamp).isEqualTo(timestamp);
    }

    @Test
    void testReportsEventTimeLagForMultiSplitRecords() {
        RecordingFlinkSourceReaderMetrics metrics = new RecordingFlinkSourceReaderMetrics();
        TableBucket tableBucket = new TableBucket(1L, 0);
        long timestamp = System.currentTimeMillis() - 10000L;
        RecordAndPos record =
                new RecordAndPos(new ScanRecord(0L, timestamp, ChangeType.INSERT, row(1, "a")), 1L);
        FlinkRecordsWithSplitIds records =
                new FlinkRecordsWithSplitIds(
                        Collections.singletonMap(
                                "split",
                                CloseableIterator.wrap(
                                        Collections.singletonList(record).iterator())),
                        Collections.singleton("split").iterator(),
                        Collections.singleton(tableBucket).iterator(),
                        new HashSet<>(),
                        metrics);

        assertThat(records.nextSplit()).isEqualTo("split");
        assertThat(records.nextRecordFromSplit()).isEqualTo(record);
        assertThat(metrics.reportedTableBucket).isEqualTo(tableBucket);
        assertThat(metrics.reportedTimestamp).isEqualTo(timestamp);
    }

    private static class RecordingFlinkSourceReaderMetrics extends FlinkSourceReaderMetrics {

        private TableBucket reportedTableBucket;
        private long reportedTimestamp = -1L;

        private RecordingFlinkSourceReaderMetrics() {
            super(InternalSourceReaderMetricGroup.mock(new MetricListener().getMetricGroup()));
        }

        @Override
        public void reportRecordEventTime(TableBucket tableBucket, long timestamp) {
            reportedTableBucket = tableBucket;
            reportedTimestamp = timestamp;
        }
    }
}
