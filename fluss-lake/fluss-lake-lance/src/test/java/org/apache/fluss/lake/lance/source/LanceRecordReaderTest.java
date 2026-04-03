/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.lance.source;

import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link LanceRecordReader}. */
class LanceRecordReaderTest extends LanceSourceTestBase {

    @Test
    void testReadAllRows() throws Exception {
        createEmptyDataset();
        long snapshotId = appendRows(Arrays.asList(row(1, "a", 0, 101L, 1001L), row(2, "b", 0, 102L, 1002L)));
        snapshotId = appendRows(Arrays.asList(row(3, "c", 1, 103L, 1003L), row(4, "d", 1, 104L, 1004L)));
        final long plannedSnapshotId = snapshotId;

        LanceLakeSource source = new LanceLakeSource(config, tablePath);
        List<LanceSplit> splits = source.createPlanner(() -> plannedSnapshotId).plan();

        List<LogRecord> records = readAll(source, splits);
        assertThat(records).hasSize(4);

        Set<Integer> ids = new HashSet<>();
        Set<String> names = new HashSet<>();
        Set<Long> offsets = new HashSet<>();
        for (LogRecord record : records) {
            InternalRow row = record.getRow();
            ids.add(row.getInt(0));
            names.add(row.getString(1).toString());
            offsets.add(record.logOffset());
            assertThat(record.timestamp()).isGreaterThan(0L);
        }

        assertThat(ids).containsExactlyInAnyOrder(1, 2, 3, 4);
        assertThat(names).containsExactlyInAnyOrder("a", "b", "c", "d");
        assertThat(offsets).containsExactlyInAnyOrder(101L, 102L, 103L, 104L);
    }

    @Test
    void testReadWithProjectFilterAndLimit() throws Exception {
        createEmptyDataset();
        long snapshotId = appendRows(Arrays.asList(row(1, "a", 0, 101L, 1001L), row(2, "b", 0, 102L, 1002L)));
        snapshotId = appendRows(Arrays.asList(row(3, "c", 1, 103L, 1003L), row(4, "d", 1, 104L, 1004L)));
        final long plannedSnapshotId = snapshotId;

        LanceLakeSource source = new LanceLakeSource(config, tablePath);
        source.withProject(new int[][] {new int[] {1}});

        RowType rowType =
                RowType.of(
                        new org.apache.fluss.types.DataType[] {new IntType(), new StringType()},
                        new String[] {"id", "name"});
        PredicateBuilder predicateBuilder = new PredicateBuilder(rowType);
        Predicate predicate = predicateBuilder.lessOrEqual(predicateBuilder.indexOf("id"), 2);
        LanceLakeSource.FilterPushDownResult pushDownResult =
                source.withFilters(Collections.singletonList(predicate));
        assertThat(pushDownResult.acceptedPredicates()).hasSize(1);
        assertThat(pushDownResult.remainingPredicates()).isEmpty();

        source.withLimit(2);

        List<LanceSplit> splits = source.createPlanner(() -> plannedSnapshotId).plan();
        List<LogRecord> records = readAll(source, splits);

        assertThat(records).hasSize(2);
        List<String> names = new ArrayList<>();
        for (LogRecord record : records) {
            InternalRow row = record.getRow();
            assertThat(row.getFieldCount()).isEqualTo(1);
            names.add(row.getString(0).toString());
        }
        assertThat(names).containsExactlyInAnyOrder("a", "b");
    }

    private List<LogRecord> readAll(LanceLakeSource source, List<LanceSplit> splits)
            throws IOException {
        List<LogRecord> all = new ArrayList<>();
        for (LanceSplit split : splits) {
            RecordReader recordReader = source.createRecordReader(() -> split);
            CloseableIterator<LogRecord> iterator = recordReader.read();
            try {
                while (iterator.hasNext()) {
                    all.add(iterator.next());
                }
            } finally {
                iterator.close();
            }
        }
        return all;
    }
}
