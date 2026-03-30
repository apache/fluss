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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.arrow.ArrowReader;
import org.apache.fluss.row.columnar.ColumnarRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.MurmurHashUtils;
import org.apache.fluss.utils.crc.Crc32C;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.apache.fluss.record.LogRecordBatchFormat.BASE_OFFSET_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.COMMIT_TIMESTAMP_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_LEADER_EPOCH;
import static org.apache.fluss.record.LogRecordBatchFormat.attributeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.batchSequenceOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.crcOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.lastOffsetDeltaOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.leaderEpochOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.recordsCountOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.statisticsDataOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.statisticsLengthOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.writeClientIdOffset;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * LogRecordBatch implementation for different magic version.
 *
 * <p>To learn more about the recordBatch format, see {@link LogRecordBatchFormat}. Supported
 * recordBatch format:
 *
 * <ul>
 *   <li>V0 => {@link LogRecordBatchFormat#LOG_MAGIC_VALUE_V0}
 *   <li>V1 => {@link LogRecordBatchFormat#LOG_MAGIC_VALUE_V1}
 *   <li>V2 => {@link LogRecordBatchFormat#LOG_MAGIC_VALUE_V2}
 * </ul>
 *
 * @since 0.1
 */
// TODO rename to MemoryLogRecordBatch
@PublicEvolving
public class DefaultLogRecordBatch implements LogRecordBatch {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogRecordBatch.class);

    public static final byte APPEND_ONLY_FLAG_MASK = 0x01;

    private MemorySegment segment;
    private int position;
    private byte magic;

    // Cache for statistics to avoid repeated parsing
    private Optional<LogRecordBatchStatistics> cachedStatistics = null;

    public void pointTo(MemorySegment segment, int position) {
        this.segment = segment;
        this.position = position;
        this.magic = segment.get(position + MAGIC_OFFSET);
        // Reset cache when pointing to new memory segment
        this.cachedStatistics = null;
    }

    public void setBaseLogOffset(long baseLogOffset) {
        segment.putLong(position + BASE_OFFSET_OFFSET, baseLogOffset);
    }

    @Override
    public byte magic() {
        return magic;
    }

    @Override
    public long commitTimestamp() {
        return segment.getLong(position + COMMIT_TIMESTAMP_OFFSET);
    }

    public void setCommitTimestamp(long timestamp) {
        segment.putLong(position + COMMIT_TIMESTAMP_OFFSET, timestamp);
    }

    public void setLeaderEpoch(int leaderEpoch) {
        if (magic >= LOG_MAGIC_VALUE_V2) {
            segment.putInt(position + leaderEpochOffset(magic), leaderEpoch);
        } else {
            throw new UnsupportedOperationException(
                    "Set leader epoch is not supported for magic v" + magic + " record batch");
        }
    }

    @Override
    public long writerId() {
        return segment.getLong(position + writeClientIdOffset(magic));
    }

    @Override
    public int batchSequence() {
        return segment.getInt(position + batchSequenceOffset(magic));
    }

    @Override
    public int leaderEpoch() {
        if (magic >= LOG_MAGIC_VALUE_V2) {
            return segment.getInt(position + leaderEpochOffset(magic));
        } else {
            return NO_LEADER_EPOCH;
        }
    }

    @Override
    public void ensureValid() {
        int sizeInBytes = sizeInBytes();
        if (sizeInBytes < recordBatchHeaderSize(magic)) {
            throw new CorruptMessageException(
                    "Record batch is corrupt (the size "
                            + sizeInBytes
                            + " is smaller than the minimum allowed overhead "
                            + recordBatchHeaderSize(magic)
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
        return sizeInBytes() >= recordBatchHeaderSize(magic) && checksum() == computeChecksum();
    }

    private long computeChecksum() {
        ByteBuffer buffer = segment.wrap(position, sizeInBytes());
        int schemaIdOffset = schemaIdOffset(magic);
        return Crc32C.compute(buffer, schemaIdOffset, sizeInBytes() - schemaIdOffset);
    }

    private byte attributes() {
        // note we're not using the byte of attributes now.
        return segment.get(attributeOffset(magic) + position);
    }

    @Override
    public long nextLogOffset() {
        return lastLogOffset() + 1;
    }

    @Override
    public long checksum() {
        return segment.getUnsignedInt(crcOffset(magic) + position);
    }

    @Override
    public short schemaId() {
        return segment.getShort(schemaIdOffset(magic) + position);
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
        return segment.getInt(lastOffsetDeltaOffset(magic) + position);
    }

    @Override
    public int sizeInBytes() {
        return LOG_OVERHEAD + segment.getInt(LENGTH_OFFSET + position);
    }

    @Override
    public int getRecordCount() {
        return segment.getInt(position + recordsCountOffset(magic));
    }

    public int getStatisticsLength() {
        if (magic < LOG_MAGIC_VALUE_V1) {
            return 0;
        }
        return segment.getInt(position + statisticsLengthOffset(magic));
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
                        context.getOutputProjectedRow(schemaId),
                        context.getVectorSchemaRoot(schemaId),
                        context.getBufferAllocator(),
                        timestamp);
            case INDEXED:
                return rowRecordIterator(
                        rowType, context.getOutputProjectedRow(schemaId), timestamp);
            case COMPACTED:
                return compactedRowRecordIterator(rowType, timestamp);
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

    private CloseableIterator<LogRecord> rowRecordIterator(
            RowType rowType, @Nullable ProjectedRow outputProjection, long timestamp) {
        DataType[] fieldTypes = rowType.getChildren().toArray(new DataType[0]);
        return new LogRecordIterator() {
            int position = DefaultLogRecordBatch.this.position + recordsDataOffset();
            int rowId = 0;

            @Override
            protected LogRecord readNext(long baseOffset) {
                IndexedLogRecord logRecord =
                        IndexedLogRecord.readFrom(
                                segment, position, baseOffset + rowId, timestamp, fieldTypes);
                rowId++;
                position += logRecord.getSizeInBytes();
                if (outputProjection == null) {
                    return logRecord;
                } else {
                    // apply projection
                    return new GenericRecord(
                            logRecord.logOffset(),
                            logRecord.timestamp(),
                            logRecord.getChangeType(),
                            outputProjection.replaceRow(logRecord.getRow()));
                }
            }

            @Override
            protected boolean ensureNoneRemaining() {
                return true;
            }

            @Override
            public void close() {}
        };
    }

    private CloseableIterator<LogRecord> compactedRowRecordIterator(
            RowType rowType, long timestamp) {
        DataType[] fieldTypes = rowType.getChildren().toArray(new DataType[0]);
        return new LogRecordIterator() {
            int position = DefaultLogRecordBatch.this.position + recordsDataOffset();
            int rowId = 0;

            @Override
            protected LogRecord readNext(long baseOffset) {
                CompactedLogRecord logRecord =
                        CompactedLogRecord.readFrom(
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
            RowType rowType,
            @Nullable ProjectedRow outputProjection,
            VectorSchemaRoot root,
            BufferAllocator allocator,
            long timestamp) {
        boolean isAppendOnly = (attributes() & APPEND_ONLY_FLAG_MASK) > 0;
        int recordsDataOffset = recordsDataOffset();
        if (isAppendOnly) {
            // append only batch, no change type vector,
            // the start of the arrow data is the beginning of the batch records
            int arrowOffset = position + recordsDataOffset;
            int arrowLength = sizeInBytes() - recordsDataOffset;
            ArrowReader reader =
                    ArrowUtils.createArrowReader(
                            segment, arrowOffset, arrowLength, root, allocator, rowType);
            return new ArrowLogRecordIterator(reader, timestamp, outputProjection) {
                @Override
                protected ChangeType getChangeType(int rowId) {
                    return ChangeType.APPEND_ONLY;
                }
            };
        } else {
            // with change type, decode the change type vector first,
            // the arrow data starts after the change type vector
            int changeTypeOffset = position + recordsDataOffset;
            ChangeTypeVector changeTypeVector =
                    new ChangeTypeVector(segment, changeTypeOffset, getRecordCount());
            int arrowOffset = changeTypeOffset + changeTypeVector.sizeInBytes();
            int arrowLength = sizeInBytes() - recordsDataOffset - changeTypeVector.sizeInBytes();
            ArrowReader reader =
                    ArrowUtils.createArrowReader(
                            segment, arrowOffset, arrowLength, root, allocator, rowType);
            return new ArrowLogRecordIterator(reader, timestamp, outputProjection) {
                @Override
                protected ChangeType getChangeType(int rowId) {
                    return changeTypeVector.getChangeType(rowId);
                }
            };
        }
    }

    /** The basic implementation for Arrow log record iterator. */
    private abstract class ArrowLogRecordIterator extends LogRecordIterator {
        private final ArrowReader reader;
        private final long timestamp;
        private int rowId = 0;
        @Nullable private final ProjectedRow outputProjection;

        private ArrowLogRecordIterator(
                ArrowReader reader, long timestamp, @Nullable ProjectedRow outputProjection) {
            this.reader = reader;
            this.timestamp = timestamp;
            this.outputProjection = outputProjection;
        }

        protected abstract ChangeType getChangeType(int rowId);

        @Override
        public boolean hasNext() {
            return rowId < reader.getRowCount();
        }

        @Override
        protected LogRecord readNext(long baseOffset) {
            ColumnarRow originalRow = reader.read(rowId);
            LogRecord record =
                    new GenericRecord(
                            baseOffset + rowId,
                            timestamp,
                            getChangeType(rowId),
                            outputProjection == null
                                    ? originalRow
                                    : outputProjection.replaceRow(originalRow));
            rowId++;
            return record;
        }

        @Override
        protected boolean ensureNoneRemaining() {
            return true;
        }

        @Override
        public void close() {
            // reader has no resources to release
        }
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
                                + magic
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

    @Override
    public Optional<LogRecordBatchStatistics> getStatistics(ReadContext context) {
        if (context == null) {
            return Optional.empty();
        }

        if (cachedStatistics != null) {
            return cachedStatistics;
        }

        cachedStatistics = parseStatistics(context);
        return cachedStatistics;
    }

    private Optional<LogRecordBatchStatistics> parseStatistics(ReadContext context) {
        try {
            if (magic < LOG_MAGIC_VALUE_V1) {
                return Optional.empty();
            }

            int statisticsLength = segment.getInt(position + statisticsLengthOffset(magic));
            if (statisticsLength == 0) {
                return Optional.empty();
            }

            RowType rowType = context.getRowType(schemaId());
            if (rowType == null) {
                LOG.debug("Skipping statistics parsing: schema {} not found", schemaId());
                return Optional.empty();
            }

            int statsDataOffset = statisticsDataOffset(magic);
            LogRecordBatchStatistics statistics =
                    LogRecordBatchStatisticsParser.parseStatistics(
                            segment, position + statsDataOffset, rowType, schemaId());
            return Optional.ofNullable(statistics);
        } catch (Exception e) {
            LOG.warn("Failed to parse statistics", e);
            return Optional.empty();
        }
    }

    /**
     * Get the offset where records data starts, relative to the batch start. For V1+, records start
     * after the fixed header + statistics data. For V0, records start right after the header.
     */
    private int recordsDataOffset() {
        int headerSize = recordBatchHeaderSize(magic);
        if (magic >= LOG_MAGIC_VALUE_V1) {
            int statsLength = segment.getInt(position + statisticsLengthOffset(magic));
            return headerSize + statsLength;
        }
        return headerSize;
    }

    // -----------------------------------------------------------------------
    // Methods for testing only
    // -----------------------------------------------------------------------

    @VisibleForTesting
    MemorySegment segment() {
        return segment;
    }

    @VisibleForTesting
    int position() {
        return position;
    }
}
