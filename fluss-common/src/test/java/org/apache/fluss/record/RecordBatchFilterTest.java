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

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Filter}. */
class RecordBatchFilterTest {

    private static final RowType TEST_ROW_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("score", DataTypes.DOUBLE()));

    private static final int TEST_SCHEMA_ID = 123;

    // Helper method to create test predicate
    private Predicate createTestPredicate() {
        PredicateBuilder builder = new PredicateBuilder(TEST_ROW_TYPE);
        return builder.greaterThan(0, 5L); // id > 5
    }

    // Helper method to create statistics for testing schema-aware behavior
    private DefaultLogRecordBatchStatistics createTestStatistics(int schemaId) {
        try {
            int[] statsMapping = new int[] {0, 1, 2}; // map all columns

            // Create collector and simulate processing some data
            LogRecordBatchStatisticsCollector collector =
                    new LogRecordBatchStatisticsCollector(TEST_ROW_TYPE, statsMapping);

            // Process some test rows to generate statistics
            List<Object[]> testData =
                    Arrays.asList(
                            new Object[] {1L, "a", 1.0},
                            new Object[] {5L, "m", 50.0},
                            new Object[] {10L, "z", 100.0});

            for (Object[] data : testData) {
                GenericRow row = new GenericRow(3);
                row.setField(0, data[0]);
                row.setField(1, BinaryString.fromString((String) data[1]));
                row.setField(2, data[2]);
                collector.processRow(row);
            }

            // Serialize the statistics to memory
            MemorySegmentOutputView outputView = new MemorySegmentOutputView(1024);

            collector.writeStatistics(outputView);

            // Get the written data
            MemorySegment segment = outputView.getMemorySegment();

            // Parse the serialized statistics back

            return LogRecordBatchStatisticsParser.parseStatistics(
                    segment, 0, TEST_ROW_TYPE, schemaId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create test statistics", e);
        }
    }

    @Test
    void testSchemaAwareStatisticsWithMatchingSchemaId() {
        Predicate testPredicate = createTestPredicate(); // id > 5
        DefaultLogRecordBatchStatistics testStats = createTestStatistics(TEST_SCHEMA_ID);

        Filter filter = new Filter(testPredicate, TEST_SCHEMA_ID);
        boolean result = filter.test(100L, testStats);

        // The statistics have max id=10 > 5, so predicate should return true (possible match)
        assertThat(result).isTrue();
    }

    @Test
    void testSchemaAwareStatisticsWithMismatchedSchemaId() {
        Predicate testPredicate = createTestPredicate(); // id > 5
        DefaultLogRecordBatchStatistics mismatchedStats =
                createTestStatistics(456); // Different schema ID

        Filter filter = new Filter(testPredicate, TEST_SCHEMA_ID);
        boolean result = filter.test(100L, mismatchedStats);

        // With mismatched schema ID, should return true (skip statistics-based filtering)
        assertThat(result).isTrue();
    }

    @Test
    void testSchemaAwareStatisticsWithNullStats() {
        Predicate testPredicate = createTestPredicate(); // id > 5

        Filter filter = new Filter(testPredicate, TEST_SCHEMA_ID);
        boolean result = filter.test(100L, (DefaultLogRecordBatchStatistics) null);

        // With null statistics, should return true (cannot filter)
        assertThat(result).isTrue();
    }
}
