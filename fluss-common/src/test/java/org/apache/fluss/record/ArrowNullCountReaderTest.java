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

package org.apache.fluss.record;

import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ArrowNullCountReader}. */
public class ArrowNullCountReaderTest {

    @Test
    void testComputeFieldNodeMapping_simpleTypes() {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("score", DataTypes.DOUBLE())
                        .build();
        int[] statsIndexMapping = {0, 1, 2};
        int[] fieldNodeMapping =
                ArrowNullCountReader.computeFieldNodeMapping(rowType, statsIndexMapping);
        // Simple types: FieldNode index == schema field index
        assertThat(fieldNodeMapping).containsExactly(0, 1, 2);
    }

    @Test
    void testComputeFieldNodeMapping_partialColumns() {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("score", DataTypes.DOUBLE())
                        .field("active", DataTypes.BOOLEAN())
                        .build();
        // Only stats for columns 0 and 2
        int[] statsIndexMapping = {0, 2};
        int[] fieldNodeMapping =
                ArrowNullCountReader.computeFieldNodeMapping(rowType, statsIndexMapping);
        assertThat(fieldNodeMapping).containsExactly(0, 2);
    }

    @Test
    void testComputeFieldNodeMapping_nestedTypes() {
        // Schema: INT, ARRAY<STRING>, DOUBLE, MAP<STRING,INT>, BOOLEAN
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("tags", DataTypes.ARRAY(DataTypes.STRING()))
                        .field("score", DataTypes.DOUBLE())
                        .field("attrs", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .field("active", DataTypes.BOOLEAN())
                        .build();

        // First, verify the Arrow schema structure to understand FieldNode layout
        Schema arrowSchema = ArrowUtils.toArrowSchema(rowType);
        // Print structure for debugging if needed
        int[] fieldNodeStarts = new int[arrowSchema.getFields().size()];
        int runningTotal = 0;
        for (int i = 0; i < arrowSchema.getFields().size(); i++) {
            fieldNodeStarts[i] = runningTotal;
            runningTotal += countFieldNodes(arrowSchema.getFields().get(i));
        }

        // Stats for [INT(col0), DOUBLE(col2), BOOLEAN(col4)]
        int[] statsIndexMapping = {0, 2, 4};
        int[] fieldNodeMapping =
                ArrowNullCountReader.computeFieldNodeMapping(rowType, statsIndexMapping);

        // INT(col0) -> FieldNode 0
        assertThat(fieldNodeMapping[0]).isEqualTo(fieldNodeStarts[0]);
        // DOUBLE(col2) -> FieldNode after ARRAY's nodes
        assertThat(fieldNodeMapping[1]).isEqualTo(fieldNodeStarts[2]);
        // BOOLEAN(col4) -> FieldNode after MAP's nodes
        assertThat(fieldNodeMapping[2]).isEqualTo(fieldNodeStarts[4]);

        // Verify concrete values based on Arrow layout:
        // INT: 1 FieldNode (index 0)
        // ARRAY<STRING>: list + string child = 2 FieldNodes (index 1, 2)
        // DOUBLE: 1 FieldNode (index 3)
        // MAP<STRING,INT>: map + entries struct + key + value = 4 FieldNodes (index 4, 5, 6, 7)
        // BOOLEAN: 1 FieldNode (index 8)
        assertThat(fieldNodeMapping).containsExactly(0, 3, 8);
    }

    /** Recursive helper to count FieldNodes for a field. */
    private static int countFieldNodes(Field field) {
        int count = 1;
        for (Field child : field.getChildren()) {
            count += countFieldNodes(child);
        }
        return count;
    }

    @Test
    void testExtractNullCounts() throws Exception {
        // Create test data with known null values:
        // col0 (INT):    null, 2, 3    -> 1 null
        // col1 (STRING): "a", null, "c" -> 1 null
        // col2 (DOUBLE): 1.0, 2.0, null -> 1 null
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("score", DataTypes.DOUBLE())
                        .build();

        List<Object[]> testData =
                Arrays.asList(
                        new Object[] {null, "a", 1.0},
                        new Object[] {2, null, 2.0},
                        new Object[] {3, "c", null});

        // Build a V2 batch with statistics using the shared test utility
        MemoryLogRecords records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        testData, rowType, 0L, DEFAULT_SCHEMA_ID);

        // Use the production code path: getStatistics() internally calls
        // readAndSetNullCountsFromArrowMetadata which uses extractNullCounts
        DefaultLogRecordBatch batch = (DefaultLogRecordBatch) records.batches().iterator().next();
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        rowType, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER);

        Optional<LogRecordBatchStatistics> statsOpt = batch.getStatistics(readContext);
        assertThat(statsOpt).isPresent();

        Long[] nullCounts = statsOpt.get().getNullCounts();
        assertThat(nullCounts).isNotNull();
        assertThat(nullCounts[0]).isEqualTo(1L); // col0: 1 null
        assertThat(nullCounts[1]).isEqualTo(1L); // col1: 1 null
        assertThat(nullCounts[2]).isEqualTo(1L); // col2: 1 null
    }
}
