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
import org.apache.fluss.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;

import static org.apache.fluss.record.LogRecordBatchFormat.STATISTICS_VERSION;

/**
 * A parser for LogRecordBatch statistics that reads statistical metadata directly from various
 * memory sources (MemorySegment, ByteBuffer, byte array) without creating intermediate heap
 * objects.
 *
 * <p>This parser supports the schema-aware statistics format and provides functionality to:
 *
 * <ul>
 *   <li>Parse complete statistics data into DefaultLogRecordBatchStatistics objects
 *   <li>Validate statistics data integrity and format compatibility
 *   <li>Calculate statistics data size from headers
 * </ul>
 *
 * <p>The statistics format includes:
 *
 * <ul>
 *   <li>Version byte for format compatibility
 *   <li>Column count and index mapping for statistics columns
 *   <li>Null counts for each statistics column
 *   <li>Min/max values stored as variable-length binary data
 * </ul>
 */
public class LogRecordBatchStatisticsParser {

    private static final Logger LOG = LoggerFactory.getLogger(LogRecordBatchStatisticsParser.class);

    // Fixed offsets within the statistics binary format.
    // Format: Version(1B) | ColumnCount(2B) | ColumnIndexes(2*N B) | NullCounts(4*N B)
    //         | MinValuesSize(4B) | MinValues(...) | MaxValuesSize(4B) | MaxValues(...)
    private static final int VERSION_OFFSET = 0;
    private static final int COLUMN_COUNT_OFFSET = 1;
    private static final int COLUMN_INDEXES_OFFSET = 3;

    /** Returns the byte offset where null counts begin, given N statistics columns. */
    private static int nullCountsOffset(int columnCount) {
        return COLUMN_INDEXES_OFFSET + 2 * columnCount;
    }

    /** Returns the byte offset where the min-values size field begins. */
    private static int minValuesSizeOffset(int columnCount) {
        return nullCountsOffset(columnCount) + 4 * columnCount;
    }

    /**
     * Parse statistics data from a memory segment using the schema-aware format.
     *
     * <p>This method reads the complete statistics structure including version, column mappings,
     * null counts, and min/max value boundaries directly from memory without copying data. The
     * returned object maintains references to the original memory segment for efficient lazy
     * evaluation of min/max values.
     *
     * @param segment The memory segment containing the serialized statistics data
     * @param position The byte position in the segment where statistics data begins
     * @param rowType The row type schema used to interpret column types and validate structure
     * @param schemaId The schema identifier for version compatibility and data interpretation
     * @return A DefaultLogRecordBatchStatistics object with zero-copy access to the data, or null
     *     if the data is invalid, corrupted, or has incompatible version
     */
    @Nullable
    public static DefaultLogRecordBatchStatistics parseStatistics(
            MemorySegment segment, int position, RowType rowType, int schemaId) {

        try {
            // Read version at fixed offset
            byte version = segment.get(position + VERSION_OFFSET);
            if (version != STATISTICS_VERSION) {
                return null;
            }

            // Read statistics column count at fixed offset
            short statisticsColumnCount = segment.getShort(position + COLUMN_COUNT_OFFSET);
            if (statisticsColumnCount <= 0) {
                return null;
            }

            // Read statistics column indexes at fixed offset
            int indexesStart = position + COLUMN_INDEXES_OFFSET;
            int[] statsIndexMapping = new int[statisticsColumnCount];
            for (int i = 0; i < statisticsColumnCount; i++) {
                statsIndexMapping[i] = segment.getShort(indexesStart + 2 * i);
            }

            // Read null counts at fixed offset
            int nullCountsStart = position + nullCountsOffset(statisticsColumnCount);
            Long[] nullCounts = new Long[statisticsColumnCount];
            for (int i = 0; i < statisticsColumnCount; i++) {
                nullCounts[i] = (long) segment.getInt(nullCountsStart + 4 * i);
            }

            // Read min values size at fixed offset
            int minSizeFieldOffset = minValuesSizeOffset(statisticsColumnCount);
            int minValuesSize = segment.getInt(position + minSizeFieldOffset);

            // Min values data starts right after the min-values size field
            int minValuesOffset = minSizeFieldOffset + 4;

            // Max values size field follows min values data
            int maxSizeFieldPos = position + minValuesOffset + minValuesSize;
            int maxValuesSize = segment.getInt(maxSizeFieldPos);

            // Max values data starts right after the max-values size field
            int maxValuesOffset = minValuesOffset + minValuesSize + 4;

            return new DefaultLogRecordBatchStatistics(
                    segment,
                    position,
                    maxValuesOffset + maxValuesSize,
                    rowType,
                    schemaId,
                    nullCounts,
                    minValuesOffset,
                    maxValuesOffset,
                    minValuesSize,
                    maxValuesSize,
                    statsIndexMapping);
        } catch (Exception e) {
            LOG.warn("Failed to parse statistics for schema {}", schemaId, e);
            return null;
        }
    }

    /**
     * Parse statistics data from a ByteBuffer by wrapping it as a MemorySegment.
     *
     * <p>This is a convenience method that wraps the ByteBuffer in a MemorySegment and delegates to
     * the primary parsing method. The ByteBuffer's position is not modified. Note that heap-backed
     * ByteBuffers incur a data copy during wrapping; direct ByteBuffers are wrapped without
     * copying.
     *
     * @param buffer The ByteBuffer containing the serialized statistics data, must not be null
     * @param rowType The row type schema used to interpret column types and validate structure
     * @param schemaId The schema identifier for version compatibility and data interpretation
     * @return A DefaultLogRecordBatchStatistics object, or null if the buffer is null/empty or
     *     contains invalid data
     */
    @Nullable
    public static DefaultLogRecordBatchStatistics parseStatistics(
            ByteBuffer buffer, RowType rowType, int schemaId) {
        if (buffer == null || buffer.remaining() == 0) {
            return null;
        }

        MemorySegment segment = wrapByteBuffer(buffer);
        return parseStatistics(segment, 0, rowType, schemaId);
    }

    /**
     * Parse statistics data from a byte array by wrapping it as a heap memory segment.
     *
     * <p>This is a convenience method that wraps the byte array in a MemorySegment and delegates to
     * the primary parsing method. The original byte array is not modified.
     *
     * @param buffer The byte array containing the serialized statistics data, must not be null
     * @param rowType The row type schema used to interpret column types and validate structure
     * @param schemaId The schema identifier for version compatibility and data interpretation
     * @return A DefaultLogRecordBatchStatistics object with zero-copy access to the data, or null
     *     if the array is null/empty or contains invalid data
     */
    @Nullable
    public static DefaultLogRecordBatchStatistics parseStatistics(
            byte[] buffer, RowType rowType, int schemaId) {
        if (buffer == null || buffer.length == 0) {
            return null;
        }

        MemorySegment segment = MemorySegment.wrap(buffer);
        return parseStatistics(segment, 0, rowType, schemaId);
    }

    /**
     * Validates whether the data in a memory segment represents well-formed statistics.
     *
     * <p>This method performs fast validation by checking:
     *
     * <ul>
     *   <li>Minimum data size requirements (at least 3 bytes for version + column count)
     *   <li>Statistics format version compatibility
     *   <li>Statistics column count against row type field count (must be positive and <= field
     *       count)
     * </ul>
     *
     * <p>This is a lightweight validation that does not fully parse the statistics data.
     *
     * @param segment The memory segment to validate, must not be null
     * @param position The byte position in the segment where statistics data begins
     * @param size The size of the data region to validate, must be >= 3
     * @param rowType The row type schema to validate column count against
     * @return true if the data appears to contain valid statistics format, false otherwise
     */
    public static boolean isValidStatistics(
            MemorySegment segment, int position, int size, RowType rowType) {
        if (segment == null || size < 3) {
            return false;
        }

        try {
            // Check version at fixed offset
            byte version = segment.get(position + VERSION_OFFSET);
            if (version != STATISTICS_VERSION) {
                return false;
            }

            // Check statistics column count at fixed offset
            int statisticsColumnCount = segment.getShort(position + COLUMN_COUNT_OFFSET);
            return statisticsColumnCount <= rowType.getFieldCount() && statisticsColumnCount > 0;

        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Validates whether a byte array contains well-formed statistics data.
     *
     * <p>This is a convenience method that wraps the byte array in a MemorySegment and delegates to
     * the primary validation method.
     *
     * @param data The byte array to validate, must not be null and at least 3 bytes long
     * @param rowType The row type schema to validate column count against
     * @return true if the array contains valid statistics format, false otherwise
     */
    public static boolean isValidStatistics(byte[] data, RowType rowType) {
        if (data == null || data.length < 3) {
            return false;
        }

        MemorySegment segment = MemorySegment.wrap(data);
        return isValidStatistics(segment, 0, data.length, rowType);
    }

    /**
     * Validates whether a ByteBuffer contains well-formed statistics data.
     *
     * <p>This is a convenience method that wraps the ByteBuffer as an off-heap MemorySegment and
     * delegates to the primary validation method. The ByteBuffer's position is not modified.
     *
     * @param buffer The ByteBuffer to validate, must not be null and have at least 3 remaining
     *     bytes
     * @param rowType The row type schema to validate column count against
     * @return true if the buffer contains valid statistics format, false otherwise
     */
    public static boolean isValidStatistics(ByteBuffer buffer, RowType rowType) {
        if (buffer == null || buffer.remaining() < 3) {
            return false;
        }

        MemorySegment segment = wrapByteBuffer(buffer);
        return isValidStatistics(segment, 0, buffer.remaining(), rowType);
    }

    /**
     * Calculates the total size of statistics data by parsing the header and size fields.
     *
     * <p>This method reads through the statistics structure to determine the complete size
     * including header, column mappings, null counts, and variable-length min/max values. It does
     * not validate data integrity beyond basic size requirements.
     *
     * @param segment The memory segment containing the statistics data, must not be null
     * @param position The byte position in the segment where statistics data begins
     * @param size The size of the available data region, must be >= 3 bytes
     * @return The total size in bytes of the complete statistics data structure, or -1 if the data
     *     is insufficient or malformed
     */
    public static int getStatisticsSize(MemorySegment segment, int position, int size) {
        if (segment == null || size < 3) {
            return -1;
        }

        try {
            // Read column count at fixed offset (skip version byte)
            short statisticsColumnCount = segment.getShort(position + COLUMN_COUNT_OFFSET);
            if (statisticsColumnCount <= 0) {
                return -1;
            }

            // Calculate offset to min-values size field using fixed layout
            int minSizeFieldOffset = minValuesSizeOffset(statisticsColumnCount);
            int minValuesSize = segment.getInt(position + minSizeFieldOffset);

            // Max-values size field follows min values data
            int maxSizeFieldOffset = minSizeFieldOffset + 4 + minValuesSize;
            int maxValuesSize = segment.getInt(position + maxSizeFieldOffset);

            // Total size: up to end of max values data
            return maxSizeFieldOffset + 4 + maxValuesSize;

        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Calculates the total size of statistics data from a byte array.
     *
     * <p>This is a convenience method that wraps the byte array in a MemorySegment and delegates to
     * the primary size calculation method.
     *
     * @param data The byte array containing the statistics data, must not be null and at least 3
     *     bytes
     * @return The total size in bytes of the complete statistics data structure, or -1 if the array
     *     is insufficient or contains malformed data
     */
    public static int getStatisticsSize(byte[] data) {
        if (data == null || data.length < 3) {
            return -1;
        }

        MemorySegment segment = MemorySegment.wrap(data);
        return getStatisticsSize(segment, 0, data.length);
    }

    /**
     * Calculates the total size of statistics data from a ByteBuffer.
     *
     * <p>This is a convenience method that wraps the ByteBuffer as an off-heap MemorySegment and
     * delegates to the primary size calculation method. The ByteBuffer's position is not modified.
     *
     * @param buffer The ByteBuffer containing the statistics data, must not be null with at least 3
     *     remaining bytes
     * @return The total size in bytes of the complete statistics data structure, or -1 if the
     *     buffer is insufficient or contains malformed data
     */
    public static int getStatisticsSize(ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() < 3) {
            return -1;
        }

        MemorySegment segment = wrapByteBuffer(buffer);
        return getStatisticsSize(segment, 0, buffer.remaining());
    }

    /**
     * Wraps a ByteBuffer into a MemorySegment, handling both direct and heap buffers correctly.
     *
     * <p>Note: For heap-backed ByteBuffers, this method copies the remaining bytes into a new byte
     * array because heap ByteBuffers cannot be wrapped directly as off-heap memory segments. Direct
     * ByteBuffers are wrapped without copying.
     *
     * @param buffer The ByteBuffer to wrap, must not be null
     * @return A MemorySegment wrapping the buffer's data
     */
    private static MemorySegment wrapByteBuffer(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            return MemorySegment.wrapOffHeapMemory(buffer);
        } else {
            byte[] bytes = new byte[buffer.remaining()];
            int pos = buffer.position();
            buffer.get(bytes);
            buffer.position(pos);
            return MemorySegment.wrap(bytes);
        }
    }
}
