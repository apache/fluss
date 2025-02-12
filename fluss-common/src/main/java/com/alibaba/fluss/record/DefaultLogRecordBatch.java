/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.exception.CorruptMessageException;
import com.alibaba.fluss.exception.OutOfOrderSequenceException;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.row.arrow.ArrowReader;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.ArrowUtils;
import com.alibaba.fluss.utils.CloseableIterator;
import com.alibaba.fluss.utils.MurmurHashUtils;
import com.alibaba.fluss.utils.crc.Crc32C;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * LogRecordBatch implementation for magic 0 and above. The schema of {@link LogRecordBatch} is
 * given below:
 *
 * <ul>
 *   RecordBatch =>
 *   <li>BaseOffset => Int64
 *   <li>Length => Int32
 *   <li>Magic => Int8
 *   <li>CommitTimestamp => Int64
 *   <li>CRC => Uint32
 *   <li>SchemaId => Int16
 *   <li>Attributes => Int8
 *   <li>LastOffsetDelta => Int32
 *   <li>WriterID => Int64
 *   <li>SequenceID => Int32
 *   <li>RecordCount => Int32
 *   <li>Records => [Record]
 * </ul>
 *
 * <p>The CRC covers the data from the schemaId to the end of the batch (i.e. all the bytes that
 * follow the CRC). It is located after the magic byte, which means that clients must parse the
 * magic byte before deciding how to interpret the bytes between the batch length and the magic
 * byte. The CRC-32C (Castagnoli) polynomial is used for the computation. CommitTimestamp is also
 * located before the CRC, because it is determined in server side.
 *
 * <p>The field 'lastOffsetDelta is used to calculate the lastOffset of the current batch as:
 * [lastOffset = baseOffset + LastOffsetDelta] instead of [lastOffset = baseOffset + recordCount -
 * 1]. The reason for introducing this field is that there might be cases where the offset delta in
 * batch does not match the recordCount. For example, when generating CDC logs for a kv table and
 * sending a batch that only contains the deletion of non-existent kvs, no CDC logs would be
 * generated. However, we need to increment the batchSequence for the corresponding writerId to make
 * sure no {@link OutOfOrderSequenceException} will be thrown. In such a case, we would generate a
 * logRecordBatch with a LastOffsetDelta of 0 but a recordCount of 0.
 *
 * <p>The current attributes are given below:
 *
 * <pre>
 * -------------------
 * |  Unused (0-8)   |
 * -------------------
 * </pre>
 *
 * @since 0.1
 */
// TODO rename to MemoryLogRecordBatch
@PublicEvolving
public class DefaultLogRecordBatch implements LogRecordBatch {
    protected static final int BASE_OFFSET_LENGTH = 8;
    public static final int LENGTH_LENGTH = 4;
    static final int MAGIC_LENGTH = 1;
    static final int COMMIT_TIMESTAMP_LENGTH = 8;
    static final int CRC_LENGTH = 4;
    static final int SCHEMA_ID_LENGTH = 2;
    static final int ATTRIBUTE_LENGTH = 1;
    static final int LAST_OFFSET_DELTA_LENGTH = 4;
    static final int WRITE_CLIENT_ID_LENGTH = 8;
    static final int BATCH_SEQUENCE_LENGTH = 4;
    static final int RECORDS_COUNT_LENGTH = 4;

    static final int BASE_OFFSET_OFFSET = 0;
    public static final int LENGTH_OFFSET = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
    static final int MAGIC_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH;
    static final int COMMIT_TIMESTAMP_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    public static final int CRC_OFFSET = COMMIT_TIMESTAMP_OFFSET + COMMIT_TIMESTAMP_LENGTH;
    protected static final int SCHEMA_ID_OFFSET = CRC_OFFSET + CRC_LENGTH;
    static final int ATTRIBUTES_OFFSET = SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH;
    static final int LAST_OFFSET_DELTA_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    static final int WRITE_CLIENT_ID_OFFSET = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
    static final int BATCH_SEQUENCE_OFFSET = WRITE_CLIENT_ID_OFFSET + WRITE_CLIENT_ID_LENGTH;
    public static final int RECORDS_COUNT_OFFSET = BATCH_SEQUENCE_OFFSET + BATCH_SEQUENCE_LENGTH;
    static final int RECORDS_OFFSET = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;

    public static final int RECORD_BATCH_HEADER_SIZE = RECORDS_OFFSET;
    public static final int ARROW_ROWKIND_OFFSET = RECORD_BATCH_HEADER_SIZE;
    public static final int LOG_OVERHEAD = LENGTH_OFFSET + LENGTH_LENGTH;

    private MemorySegment segment;
    private int position;

    public void pointTo(MemorySegment segment, int position) {
        this.segment = segment;
        this.position = position;
    }

    public void setBaseLogOffset(long baseLogOffset) {
        segment.putLong(position + BASE_OFFSET_OFFSET, baseLogOffset);
    }

    @Override
    public byte magic() {
        return segment.get(position + MAGIC_OFFSET);
    }

    @Override
    public long commitTimestamp() {
        return segment.getLong(position + COMMIT_TIMESTAMP_OFFSET);
    }

    public void setCommitTimestamp(long timestamp) {
        segment.putLong(position + COMMIT_TIMESTAMP_OFFSET, timestamp);
    }

    @Override
    public long writerId() {
        return segment.getLong(position + WRITE_CLIENT_ID_OFFSET);
    }

    @Override
    public int batchSequence() {
        return segment.getInt(position + BATCH_SEQUENCE_OFFSET);
    }

    @Override
    public void ensureValid() {
        int sizeInBytes = sizeInBytes();
        if (sizeInBytes < RECORD_BATCH_HEADER_SIZE) {
            throw new CorruptMessageException(
                    "Record batch is corrupt (the size "
                            + sizeInBytes
                            + " is smaller than the minimum allowed overhead "
                            + RECORD_BATCH_HEADER_SIZE
                            + ")");
        }

        if (!isValid()) {
            throw new CorruptMessageException(
                    "Record batch is corrupt (stored crc = "
                            + checksum()
                            + ", computed crc = "
                            + computeChecksum()
                            + ")");
        }
    }

    @Override
    public boolean isValid() {
        return sizeInBytes() >= RECORD_BATCH_HEADER_SIZE && checksum() == computeChecksum();
    }

    private long computeChecksum() {
        ByteBuffer buffer = segment.wrap(position, sizeInBytes());
        return Crc32C.compute(buffer, SCHEMA_ID_OFFSET, sizeInBytes() - SCHEMA_ID_OFFSET);
    }

    private byte attributes() {
        // note we're not using the byte of attributes now.
        return segment.get(ATTRIBUTES_OFFSET + position);
    }

    @Override
    public long nextLogOffset() {
        return lastLogOffset() + 1;
    }

    @Override
    public long checksum() {
        return segment.getUnsignedInt(CRC_OFFSET + position);
    }

    @Override
    public short schemaId() {
        return segment.getShort(SCHEMA_ID_OFFSET + position);
    }

    @Override
    public long baseLogOffset() {
        return segment.getLong(BASE_OFFSET_OFFSET + position);
    }

    @Override
    public long lastLogOffset() {
        return baseLogOffset() + lastOffsetDelta();
    }

    private int lastOffsetDelta() {
        return segment.getInt(LAST_OFFSET_DELTA_OFFSET + position);
    }

    @Override
    public int sizeInBytes() {
        return LOG_OVERHEAD + segment.getInt(LENGTH_OFFSET + position);
    }

    @Override
    public int getRecordCount() {
        return segment.getInt(RECORDS_COUNT_OFFSET + position);
    }

    @Override
    public CloseableIterator<LogRecord> records(ReadContext context) {
        if (getRecordCount() == 0) {
            return CloseableIterator.emptyIterator();
        }

        int schemaId = schemaId();
        long timestamp = commitTimestamp();
        LogFormat logFormat = context.getLogFormat();
        RowType rowType = context.getRowType(schemaId);
        switch (logFormat) {
            case ARROW:
                return columnRecordIterator(
                        rowType,
                        context.getVectorSchemaRoot(schemaId),
                        context.getBufferAllocator(),
                        timestamp);
            case INDEXED:
                return rowRecordIterator(rowType, timestamp);
            default:
                throw new IllegalArgumentException("Unsupported log format: " + logFormat);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultLogRecordBatch that = (DefaultLogRecordBatch) o;
        int sizeInBytes = sizeInBytes();
        return sizeInBytes == that.sizeInBytes()
                && segment.equalTo(that.segment, position, that.position, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, position, sizeInBytes());
    }

    private CloseableIterator<LogRecord> rowRecordIterator(RowType rowType, long timestamp) {
        DataType[] fieldTypes = rowType.getChildren().toArray(new DataType[0]);
        return new LogRecordIterator() {
            int position = DefaultLogRecordBatch.this.position + RECORD_BATCH_HEADER_SIZE;
            int rowId = 0;

            @Override
            protected LogRecord readNext(long baseOffset) {
                IndexedLogRecord logRecord =
                        IndexedLogRecord.readFrom(
                                segment, position, baseOffset + rowId, timestamp, fieldTypes);
                rowId++;
                position += logRecord.getSizeInBytes();
                return logRecord;
            }

            @Override
            protected boolean ensureNoneRemaining() {
                return true;
            }

            @Override
            public void close() {}
        };
    }

    private CloseableIterator<LogRecord> columnRecordIterator(
            RowType rowType, VectorSchemaRoot root, BufferAllocator allocator, long timestamp) {
        int rowKindOffset = position + ARROW_ROWKIND_OFFSET;
        RowKindVector rowKindVector = new RowKindVector(segment, rowKindOffset, getRecordCount());
        int arrowOffset = rowKindOffset + rowKindVector.sizeInBytes();
        int arrowLength = sizeInBytes() - ARROW_ROWKIND_OFFSET - rowKindVector.sizeInBytes();
        ArrowReader reader =
                ArrowUtils.createArrowReader(
                        segment, arrowOffset, arrowLength, root, allocator, rowType);
        return new LogRecordIterator() {
            int rowId = 0;

            @Override
            public boolean hasNext() {
                return rowId < reader.getRowCount();
            }

            @Override
            protected LogRecord readNext(long baseOffset) {
                RowKind rowKind = rowKindVector.getRowKind(rowId);
                LogRecord record =
                        new GenericRecord(
                                baseOffset + rowId, timestamp, rowKind, reader.read(rowId));
                rowId++;
                return record;
            }

            @Override
            protected boolean ensureNoneRemaining() {
                return true;
            }

            @Override
            public void close() {
                reader.close();
            }
        };
    }

    /** Default log record iterator. */
    private abstract class LogRecordIterator implements CloseableIterator<LogRecord> {
        private final long baseOffset;
        private final int numRecords;
        private int readRecords = 0;

        public LogRecordIterator() {
            this.baseOffset = baseLogOffset();
            int numRecords = getRecordCount();
            if (numRecords < 0) {
                throw new IllegalArgumentException(
                        "Found invalid record count "
                                + numRecords
                                + " in magic v"
                                + magic()
                                + " batch");
            }
            this.numRecords = numRecords;
        }

        @Override
        public boolean hasNext() {
            return readRecords < numRecords;
        }

        @Override
        public LogRecord next() {
            if (readRecords >= numRecords) {
                throw new NoSuchElementException();
            }

            readRecords++;
            LogRecord rec = readNext(baseOffset);
            if (readRecords == numRecords) {
                // Validate that the actual size of the batch is equal to declared size
                // by checking that after reading declared number of items, there no items left
                // (overflow case, i.e. reading past buffer end is checked elsewhere).
                if (!ensureNoneRemaining()) {
                    throw new IllegalArgumentException(
                            "Incorrect declared batch size, records still remaining in file");
                }
            }
            return rec;
        }

        protected abstract LogRecord readNext(long baseOffset);

        protected abstract boolean ensureNoneRemaining();

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
