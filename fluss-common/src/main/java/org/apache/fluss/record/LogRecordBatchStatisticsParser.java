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
import org.apache.fluss.memory.MemorySegmentInputView;
import org.apache.fluss.types.RowType;

import java.io.IOException;
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
    public static DefaultLogRecordBatchStatistics parseStatistics(
            MemorySegment segment, int position, RowType rowType, int schemaId) {

        try {
            MemorySegmentInputView inputView = new MemorySegmentInputView(segment, position);
            int currentPos = position;

            // Read version
            byte version = inputView.readByte();
            currentPos += 1;
            if (version != STATISTICS_VERSION) {
                return null;
            }

            // Read statistics column count
            int statisticsColumnCount = inputView.readShort();
            currentPos += 2;

            // Read statistics column indexes
            int[] statsIndexMapping = new int[statisticsColumnCount];
            for (int i = 0; i < statisticsColumnCount; i++) {
                statsIndexMapping[i] = inputView.readShort();
                currentPos += 2;
            }

            // Read null counts for statistics columns
            Long[] nullCounts = new Long[statisticsColumnCount];
            for (int i = 0; i < statisticsColumnCount; i++) {
                nullCounts[i] = (long) inputView.readInt();
                currentPos += 4;
            }

            // Read min values size
            int minValuesSize = inputView.readInt();
            currentPos += 4;

            // Calculate min values offset
            int minValuesOffset = currentPos - position;

            if (minValuesSize > 0) {
                currentPos += minValuesSize;
                // Advance the inputView position to match currentPos
                for (int i = 0; i < minValuesSize; i++) {
                    inputView.readByte();
                }
            }

            // Read max values size
            int maxValuesSize = inputView.readInt();
            currentPos += 4;

            // Calculate max values offset
            int maxValuesOffset = currentPos - position;

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
            return null;
        }
    }

    /**
     * Parse statistics data from a ByteBuffer by wrapping it as an off-heap memory segment.
     *
     * <p>This is a convenience method that wraps the ByteBuffer in a MemorySegment and delegates to
     * the primary parsing method. The ByteBuffer's position is not modified.
     *
     * @param buffer The ByteBuffer containing the serialized statistics data, must not be null
     * @param rowType The row type schema used to interpret column types and validate structure
     * @param schemaId The schema identifier for version compatibility and data interpretation
     * @return A DefaultLogRecordBatchStatistics object with zero-copy access to the data, or null
     *     if the buffer is null/empty or contains invalid data
     */
    public static DefaultLogRecordBatchStatistics parseStatistics(
            ByteBuffer buffer, RowType rowType, int schemaId) {
        if (buffer == null || buffer.remaining() == 0) {
            return null;
        }

        MemorySegment segment = MemorySegment.wrapOffHeapMemory(buffer);
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
            MemorySegmentInputView inputView = new MemorySegmentInputView(segment, position);

            // Check version
            byte version = inputView.readByte();
            if (version != STATISTICS_VERSION) {
                return false;
            }

            // Check statistics column count
            int statisticsColumnCount = inputView.readShort();
            return statisticsColumnCount <= rowType.getFieldCount() && statisticsColumnCount > 0;

        } catch (IOException e) {
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

        MemorySegment segment = MemorySegment.wrapOffHeapMemory(buffer);
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
            MemorySegmentInputView inputView = new MemorySegmentInputView(segment, position);
            int currentPos = position;

            // Skip version
            inputView.readByte(); // version
            currentPos += 1;

            // Read statistics column count
            int statisticsColumnCount = inputView.readShort();
            currentPos += 2;

            // Skip statistics column indexes (2 bytes each)
            for (int i = 0; i < statisticsColumnCount; i++) {
                inputView.readShort();
            }
            currentPos += statisticsColumnCount * 2;

            // Skip null counts (4 bytes per statistics column)
            int nullCountsSize = statisticsColumnCount * 4;
            for (int i = 0; i < nullCountsSize; i++) {
                inputView.readByte();
            }
            currentPos += nullCountsSize;

            // Read min values size
            int minValuesSize = inputView.readInt();
            currentPos += 4;

            // Skip min values
            if (minValuesSize > 0) {
                for (int i = 0; i < minValuesSize; i++) {
                    inputView.readByte();
                }
                currentPos += minValuesSize;
            }

            // Read max values size
            int maxValuesSize = inputView.readInt();
            currentPos += 4;

            // Skip max values
            if (maxValuesSize > 0) {
                for (int i = 0; i < maxValuesSize; i++) {
                    inputView.readByte();
                }
                currentPos += maxValuesSize;
            }

            return currentPos - position;

        } catch (IOException e) {
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

        MemorySegment segment = MemorySegment.wrapOffHeapMemory(buffer);
        return getStatisticsSize(segment, 0, buffer.remaining());
    }
}
