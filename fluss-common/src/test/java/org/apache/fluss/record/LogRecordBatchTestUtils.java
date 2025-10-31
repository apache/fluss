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

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.RowType;

import java.util.List;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;

/**
 * Test utility class for generating LogRecordBatchStatistics. This utility class can be reused in
 * other test cases.
 */
public class LogRecordBatchTestUtils {

    // Helper method to create stats index mapping for all columns
    public static int[] createAllColumnsStatsMapping(RowType rowType) {
        int[] statsIndexMapping = new int[rowType.getFieldCount()];
        for (int i = 0; i < statsIndexMapping.length; i++) {
            statsIndexMapping[i] = i;
        }
        return statsIndexMapping;
    }

    /**
     * Create a reusable utility method for generating LogRecordBatch with statistics. This method
     * can be reused in other test cases.
     *
     * @param data Test data
     * @param rowType Row type
     * @param baseOffset Base offset
     * @param schemaId Schema ID
     * @return MemoryLogRecords containing statistics
     * @throws Exception If creation fails
     */
    public static MemoryLogRecords createLogRecordsWithStatistics(
            List<Object[]> data, RowType rowType, long baseOffset, int schemaId) throws Exception {

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowWriterPool writerPool = new ArrowWriterPool(allocator)) {

            ArrowWriter writer =
                    writerPool.getOrCreateWriter(
                            1L, schemaId, Integer.MAX_VALUE, rowType, DEFAULT_COMPRESSION);

            // Create statistics collector for the writer's schema
            LogRecordBatchStatisticsCollector statisticsCollector =
                    new LogRecordBatchStatisticsCollector(
                            writer.getSchema(), createAllColumnsStatsMapping(writer.getSchema()));

            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            baseOffset,
                            CURRENT_LOG_MAGIC_VALUE,
                            schemaId,
                            writer,
                            new ManagedPagedOutputView(new TestingMemorySegmentPool(10 * 1024)),
                            statisticsCollector);

            // Convert data to InternalRow and add to builder
            List<InternalRow> rows =
                    data.stream()
                            .map(DataTestUtils::row)
                            .collect(java.util.stream.Collectors.toList());
            for (InternalRow row : rows) {
                builder.append(ChangeType.APPEND_ONLY, row);
            }

            builder.setWriterState(1L, 0);
            builder.close();

            MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
            memoryLogRecords.ensureValid(CURRENT_LOG_MAGIC_VALUE);

            return memoryLogRecords;
        }
    }

    /**
     * Create a reusable utility method for generating LogRecordBatch with statistics (using default
     * parameters).
     *
     * @param data Test data
     * @param rowType Row type
     * @return MemoryLogRecords containing statistics
     * @throws Exception If creation fails
     */
    public static MemoryLogRecords createLogRecordsWithStatistics(
            List<Object[]> data, RowType rowType) throws Exception {
        return createLogRecordsWithStatistics(data, rowType, 0L, 1);
    }

    /**
     * Create a reusable utility method for generating LogRecordBatch with statistics (using custom
     * compression).
     *
     * @param data Test data
     * @param rowType Row type
     * @param baseOffset Base offset
     * @param schemaId Schema ID
     * @param compressionInfo Compression information
     * @return MemoryLogRecords containing statistics
     * @throws Exception If creation fails
     */
    public static MemoryLogRecords createLogRecordsWithStatistics(
            List<Object[]> data,
            RowType rowType,
            long baseOffset,
            int schemaId,
            ArrowCompressionInfo compressionInfo)
            throws Exception {

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowWriterPool writerPool = new ArrowWriterPool(allocator)) {

            ArrowWriter writer =
                    writerPool.getOrCreateWriter(
                            1L, schemaId, Integer.MAX_VALUE, rowType, compressionInfo);

            // Create statistics collector for the writer's schema
            LogRecordBatchStatisticsCollector statisticsCollector =
                    new LogRecordBatchStatisticsCollector(
                            writer.getSchema(), createAllColumnsStatsMapping(writer.getSchema()));

            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            baseOffset,
                            CURRENT_LOG_MAGIC_VALUE,
                            schemaId,
                            writer,
                            new ManagedPagedOutputView(new TestingMemorySegmentPool(10 * 1024)),
                            statisticsCollector);

            // Convert data to InternalRow and add to builder
            List<InternalRow> rows =
                    data.stream()
                            .map(DataTestUtils::row)
                            .collect(java.util.stream.Collectors.toList());
            for (InternalRow row : rows) {
                builder.append(ChangeType.APPEND_ONLY, row);
            }

            builder.setWriterState(1L, 0);
            builder.close();

            MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
            memoryLogRecords.ensureValid(CURRENT_LOG_MAGIC_VALUE);

            return memoryLogRecords;
        }
    }

    /**
     * Create a reusable utility method for generating LogRecordBatch with statistics (using custom
     * ChangeType).
     *
     * @param data Test data
     * @param rowType Row type
     * @param baseOffset Base offset
     * @param schemaId Schema ID
     * @param changeTypes List of change types
     * @return MemoryLogRecords containing statistics
     * @throws Exception If creation fails
     */
    public static MemoryLogRecords createLogRecordsWithStatistics(
            List<Object[]> data,
            RowType rowType,
            long baseOffset,
            int schemaId,
            List<ChangeType> changeTypes)
            throws Exception {

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowWriterPool writerPool = new ArrowWriterPool(allocator)) {

            ArrowWriter writer =
                    writerPool.getOrCreateWriter(
                            1L, schemaId, Integer.MAX_VALUE, rowType, DEFAULT_COMPRESSION);

            // Create statistics collector for the writer's schema
            LogRecordBatchStatisticsCollector statisticsCollector =
                    new LogRecordBatchStatisticsCollector(
                            writer.getSchema(), createAllColumnsStatsMapping(writer.getSchema()));

            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            baseOffset,
                            CURRENT_LOG_MAGIC_VALUE,
                            schemaId,
                            writer,
                            new ManagedPagedOutputView(new TestingMemorySegmentPool(10 * 1024)),
                            statisticsCollector);

            // Convert data to InternalRow and add to builder
            List<InternalRow> rows =
                    data.stream()
                            .map(DataTestUtils::row)
                            .collect(java.util.stream.Collectors.toList());
            for (int i = 0; i < rows.size(); i++) {
                ChangeType changeType =
                        i < changeTypes.size() ? changeTypes.get(i) : ChangeType.APPEND_ONLY;
                builder.append(changeType, rows.get(i));
            }

            builder.setWriterState(1L, 0);
            builder.close();

            MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
            memoryLogRecords.ensureValid(CURRENT_LOG_MAGIC_VALUE);

            return memoryLogRecords;
        }
    }

    /**
     * Create a reusable utility method for generating LogRecordBatch with statistics (using custom
     * Writer state).
     *
     * @param data Test data
     * @param rowType Row type
     * @param baseOffset Base offset
     * @param schemaId Schema ID
     * @param writerId Writer ID
     * @param batchSequence Batch sequence number
     * @return MemoryLogRecords containing statistics
     * @throws Exception If creation fails
     */
    public static MemoryLogRecords createLogRecordsWithStatistics(
            List<Object[]> data,
            RowType rowType,
            long baseOffset,
            int schemaId,
            long writerId,
            int batchSequence)
            throws Exception {

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowWriterPool writerPool = new ArrowWriterPool(allocator)) {

            ArrowWriter writer =
                    writerPool.getOrCreateWriter(
                            1L, schemaId, Integer.MAX_VALUE, rowType, DEFAULT_COMPRESSION);

            // Create statistics collector for the writer's schema
            LogRecordBatchStatisticsCollector statisticsCollector =
                    new LogRecordBatchStatisticsCollector(
                            writer.getSchema(), createAllColumnsStatsMapping(writer.getSchema()));

            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            baseOffset,
                            CURRENT_LOG_MAGIC_VALUE,
                            schemaId,
                            writer,
                            new ManagedPagedOutputView(new TestingMemorySegmentPool(10 * 1024)),
                            statisticsCollector);

            // Convert data to InternalRow and add to builder
            List<InternalRow> rows =
                    data.stream()
                            .map(DataTestUtils::row)
                            .collect(java.util.stream.Collectors.toList());
            for (InternalRow row : rows) {
                builder.append(ChangeType.APPEND_ONLY, row);
            }

            builder.setWriterState(writerId, batchSequence);
            builder.close();

            MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
            memoryLogRecords.ensureValid(CURRENT_LOG_MAGIC_VALUE);

            return memoryLogRecords;
        }
    }
}
