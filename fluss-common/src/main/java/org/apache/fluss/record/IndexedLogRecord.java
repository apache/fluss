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
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.MurmurHashUtils;

import java.io.IOException;

import static org.apache.fluss.record.DefaultLogRecordBatch.LENGTH_LENGTH;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an immutable log record and can be directly persisted. The schema is as follows:
 *
 * <ul>
 *   <li>Length => int32
 *   <li>Attributes => Int8
 *   <li>Value => {@link InternalRow}
 * </ul>
 *
 * <p>The current record attributes are depicted below:
 *
 * <p>----------- | ChangeType (0-3) | Unused (4-7) |---------------
 *
 * <p>The offset compute the difference relative to the base offset and of the batch that this
 * record is contained in.
 *
 * @since 0.1
 */
@PublicEvolving
public class IndexedLogRecord implements LogRecord {

    private static final int ATTRIBUTES_LENGTH = 1;

    private final long logOffset;
    private final long timestamp;
    private final DataType[] fieldTypes;

    private MemorySegment segment;
    private int offset;
    private int sizeInBytes;

    IndexedLogRecord(long logOffset, long timestamp, DataType[] fieldTypes) {
        this.logOffset = logOffset;
        this.fieldTypes = fieldTypes;
        this.timestamp = timestamp;
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

        IndexedLogRecord that = (IndexedLogRecord) o;
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
        // TODO currently, we only support indexed row.
        return deserializeInternalRow(
                sizeInBytes - rowOffset,
                segment,
                offset + rowOffset,
                fieldTypes,
                LogFormat.INDEXED);
    }

    /** Write the record to input `target` and return its size. */
    public static int writeTo(OutputView outputView, ChangeType changeType, IndexedRow row)
            throws IOException {
        int sizeInBytes = calculateSizeInBytes(row);

        // TODO using varint instead int to reduce storage size.
        // write record total bytes size.
        outputView.writeInt(sizeInBytes);

        // write attributes.
        outputView.writeByte(changeType.toByteValue());

        // write internal row.
        serializeInternalRow(outputView, row);

        return sizeInBytes + LENGTH_LENGTH;
    }

    public static IndexedLogRecord readFrom(
            MemorySegment segment,
            int position,
            long logOffset,
            long logTimestamp,
            DataType[] colTypes) {
        int sizeInBytes = segment.getInt(position);
        IndexedLogRecord logRecord = new IndexedLogRecord(logOffset, logTimestamp, colTypes);
        logRecord.pointTo(segment, position, sizeInBytes + LENGTH_LENGTH);
        return logRecord;
    }

    public static int sizeOf(BinaryRow row) {
        int sizeInBytes = calculateSizeInBytes(row);
        return sizeInBytes + LENGTH_LENGTH;
    }

    private static int calculateSizeInBytes(BinaryRow row) {
        int size = 1; // always one byte for attributes
        size += row.getSizeInBytes();
        return size;
    }

    private static void serializeInternalRow(OutputView outputView, IndexedRow row)
            throws IOException {
        IndexedRowWriter.serializeIndexedRow(row, outputView);
    }

    private static InternalRow deserializeInternalRow(
            int length,
            MemorySegment segment,
            int position,
            DataType[] fieldTypes,
            LogFormat logFormat) {
        if (logFormat == LogFormat.INDEXED) {
            IndexedRow indexedRow = new IndexedRow(fieldTypes);
            indexedRow.pointTo(segment, position, length);
            return indexedRow;
        } else {
            throw new IllegalArgumentException(
                    "No such internal row deserializer for: " + logFormat);
        }
    }
}
