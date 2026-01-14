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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.MurmurHashUtils;

import java.io.IOException;

import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_LENGTH;

/**
 * An immutable log record for {@link CompactedRow} which can be directly persisted. The on-wire
 * schema is identical to IndexedLogRecord but the row payload uses the CompactedRow binary format:
 *
 * <ul>
 *   <li>Length => int32 (total number of bytes following this length field)
 *   <li>Attributes => int8 (low 4 bits encode {@link ChangeType})
 *   <li>Value => {@link CompactedRow} (bytes in compacted row format)
 * </ul>
 *
 * <p>Differences vs {@link IndexedLogRecord}: - Uses CompactedRow encoding which is space-optimized
 * (VLQ for ints/longs, per-row null bitset) and trades CPU for smaller storage; random access to
 * fields is not supported without decoding. - Deserialization is lazy: we wrap the underlying bytes
 * in a CompactedRow with a {@link CompactedRowDeserializer} and only decode to object values when a
 * field is accessed. - The record header (Length + Attributes) layout and attribute semantics are
 * the same.
 *
 * <p>The offset computes the difference relative to the base offset of the batch containing this
 * record.
 *
 * @since 0.8
 */
@PublicEvolving
public class CompactedLogRecord implements LogRecord {

    private static final int ATTRIBUTES_LENGTH = 1;

    private final long logOffset;
    private final long timestamp;
    private final DataType[] fieldTypes;

    private MemorySegment segment;
    private int offset;
    private int sizeInBytes;

    CompactedLogRecord(long logOffset, long timestamp, DataType[] fieldTypes) {
        this.logOffset = logOffset;
        this.timestamp = timestamp;
        this.fieldTypes = fieldTypes;
    }

    private void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
        this.segment = segment;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactedLogRecord that = (CompactedLogRecord) o;
        return sizeInBytes == that.sizeInBytes
                && segment.equalTo(that.segment, offset, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, offset, sizeInBytes);
    }

    @Override
    public long logOffset() {
        return logOffset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public ChangeType getChangeType() {
        byte attributes = segment.get(offset + LENGTH_LENGTH);
        return ChangeType.fromByteValue(attributes);
    }

    @Override
    public InternalRow getRow() {
        int rowOffset = LENGTH_LENGTH + ATTRIBUTES_LENGTH;
        return LogRecord.deserializeInternalRow(
                sizeInBytes - rowOffset,
                segment,
                offset + rowOffset,
                fieldTypes,
                LogFormat.COMPACTED);
    }

    /** Write the record to output and return total bytes written including length field. */
    public static int writeTo(OutputView outputView, ChangeType changeType, CompactedRow row)
            throws IOException {
        int sizeInBytes = calculateSizeInBytes(row);
        // write record total bytes size (excluding this int itself)
        outputView.writeInt(sizeInBytes);
        // write attributes
        outputView.writeByte(changeType.toByteValue());
        // write row payload
        CompactedRowWriter.serializeCompactedRow(row, outputView);
        return sizeInBytes + LENGTH_LENGTH;
    }

    public static CompactedLogRecord readFrom(
            MemorySegment segment,
            int position,
            long logOffset,
            long logTimestamp,
            DataType[] colTypes) {
        int sizeInBytes = segment.getInt(position);
        CompactedLogRecord logRecord = new CompactedLogRecord(logOffset, logTimestamp, colTypes);
        logRecord.pointTo(segment, position, sizeInBytes + LENGTH_LENGTH);
        return logRecord;
    }

    public static int sizeOf(BinaryRow row) {
        int sizeInBytes = calculateSizeInBytes(row);
        return sizeInBytes + LENGTH_LENGTH;
    }

    private static int calculateSizeInBytes(BinaryRow row) {
        int size = 1; // one byte for attributes
        size += row.getSizeInBytes();
        return size;
    }
}
