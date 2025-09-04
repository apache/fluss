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

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.FileRegionBytesView;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.Optional;

import static org.apache.fluss.record.LogRecordBatchFormat.BASE_OFFSET_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.STATISTICS_FLAG_MASK;
import static org.apache.fluss.record.LogRecordBatchFormat.arrowChangeTypeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.attributeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.statisticsOffsetOffset;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A log input stream which is backed by a {@link FileChannel}. */
public class FileLogInputStream
        implements LogInputStream<FileLogInputStream.FileChannelLogRecordBatch> {
    private static final Logger LOG = LoggerFactory.getLogger(FileLogInputStream.class);

    private int position;
    private final int end;
    private final FileLogRecords fileRecords;
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);

    /** Create a new log input stream over the FileChannel. */
    FileLogInputStream(FileLogRecords records, int start, int end) {
        this.fileRecords = records;
        this.position = start;
        this.end = end;
        this.logHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public FileChannelLogRecordBatch nextBatch() throws IOException {
        FileChannel channel = fileRecords.channel();
        if (position >= end - HEADER_SIZE_UP_TO_MAGIC) {
            return null;
        }

        logHeaderBuffer.rewind();
        FileUtils.readFullyOrFail(channel, logHeaderBuffer, position, "log header");

        logHeaderBuffer.rewind();
        long offset = logHeaderBuffer.getLong(BASE_OFFSET_OFFSET);
        int length = logHeaderBuffer.getInt(LENGTH_OFFSET);

        if (position > end - LOG_OVERHEAD - length) {
            return null;
        }

        byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        FileChannelLogRecordBatch batch =
                new FileChannelLogRecordBatch(offset, magic, fileRecords, position, length);

        position += batch.sizeInBytes();
        return batch;
    }

    /**
     * Log entry backed by an underlying FileChannel. This allows iteration over the record batches
     * without needing to read the record data into memory until it is needed. The downside is that
     * entries will generally no longer be readable when the underlying channel is closed.
     */
    public static class FileChannelLogRecordBatch implements LogRecordBatch {
        protected final long offset;
        protected final byte magic;
        protected final FileLogRecords fileRecords;
        protected final int position;
        protected final int batchSize;

        private DefaultLogRecordBatch fullBatch;
        private DefaultLogRecordBatch batchHeader;
        private ByteBuffer cachedHeaderBuffer;

        // Cache for statistics to avoid repeated parsing
        private Optional<LogRecordBatchStatistics> cachedStatistics = null;

        FileChannelLogRecordBatch(
                long offset, byte magic, FileLogRecords fileRecords, int position, int batchSize) {
            this.offset = offset;
            this.magic = magic;
            this.fileRecords = fileRecords;
            this.position = position;
            this.batchSize = batchSize;
        }

        @Override
        public long checksum() {
            return loadBatchHeader().checksum();
        }

        @Override
        public short schemaId() {
            return loadBatchHeader().schemaId();
        }

        @Override
        public long baseLogOffset() {
            return offset;
        }

        public int position() {
            return position;
        }

        public BytesView getBytesView() {
            return getBytesView(false);
        }

        /**
         * Get the BytesView of this record batch.
         *
         * @param trimStatistics whether to trim statistics data from the batch
         * @return the BytesView of this record batch, possibly without statistics
         */
        public BytesView getBytesView(boolean trimStatistics) {
            if (!trimStatistics || magic < LOG_MAGIC_VALUE_V2) {
                // No trimming needed, or statistics not supported in this version
                return new FileRegionBytesView(fileRecords.channel(), position, sizeInBytes());
            }

            // Check if this batch has statistics
            DefaultLogRecordBatch header = loadBatchHeader();
            int statisticsOffset = header.getStatisticsOffset();

            if (statisticsOffset == 0) {
                // No statistics present
                return new FileRegionBytesView(fileRecords.channel(), position, sizeInBytes());
            }

            // Create a modified header with correct length and statisticsOffset fields
            return createTrimmedBytesView(statisticsOffset);
        }

        /**
         * Create a BytesView with statistics trimmed by modifying the header fields. This
         * implementation follows the same logic as FileLogProjection.projectRecordBatch.
         *
         * @param originalStatisticsOffset the original statistics offset in the header
         * @return a BytesView with modified header and trimmed data
         */
        private BytesView createTrimmedBytesView(int originalStatisticsOffset) {
            try {
                int headerSize = recordBatchHeaderSize(magic);

                // Calculate the new batch size without statistics
                int newBatchSizeWithoutStatistics = originalStatisticsOffset;
                // Load the original header
                byte[] modifiedHeaderBytes = new byte[headerSize];
                cachedHeaderBuffer.rewind();
                cachedHeaderBuffer.get(modifiedHeaderBytes);
                ByteBuffer modifiedHeader = ByteBuffer.wrap(modifiedHeaderBytes);
                modifiedHeader.order(ByteOrder.LITTLE_ENDIAN);

                // Update the length field (LENGTH field should exclude LOG_OVERHEAD)
                modifiedHeader.position(LENGTH_OFFSET);
                modifiedHeader.putInt(newBatchSizeWithoutStatistics - LOG_OVERHEAD);

                // For V2 format, clear statistics information
                if (magic == LOG_MAGIC_VALUE_V2) {
                    // Set StatisticsOffset to 0 (no statistics)
                    modifiedHeader.position(statisticsOffsetOffset(magic));
                    modifiedHeader.putInt(0);

                    // Clear statistics flag from attributes
                    modifiedHeader.position(attributeOffset(magic));
                    byte attributes = modifiedHeader.get();
                    modifiedHeader.position(attributeOffset(magic));
                    modifiedHeader.put((byte) (attributes & ~STATISTICS_FLAG_MASK));
                }

                // Build the composite BytesView
                MultiBytesView.Builder builder = MultiBytesView.builder();

                // Add the modified header
                builder.addBytes(modifiedHeaderBytes);

                // Add the data part (from after LOG_OVERHEAD to before statistics)
                int dataStartPos = position + headerSize();
                int dataSize = originalStatisticsOffset - arrowChangeTypeOffset(magic);
                if (dataSize > 0) {
                    builder.addBytes(fileRecords.channel(), dataStartPos, dataSize);
                }

                return builder.build();
            } catch (Exception e) {
                LOG.warn("Failed to create trimmed BytesView, fallback to original", e);
                // Fallback to original implementation if something goes wrong
                return new FileRegionBytesView(
                        fileRecords.channel(), position, originalStatisticsOffset);
            }
        }

        @Override
        public byte magic() {
            return magic;
        }

        @Override
        public long commitTimestamp() {
            return loadBatchHeader().commitTimestamp();
        }

        @Override
        public long nextLogOffset() {
            return lastLogOffset() + 1;
        }

        @Override
        public long writerId() {
            return loadBatchHeader().writerId();
        }

        @Override
        public int batchSequence() {
            return loadBatchHeader().batchSequence();
        }

        @Override
        public int leaderEpoch() {
            return loadBatchHeader().leaderEpoch();
        }

        @Override
        public long lastLogOffset() {
            return loadBatchHeader().lastLogOffset();
        }

        @Override
        public int getRecordCount() {
            return loadBatchHeader().getRecordCount();
        }

        @Override
        public CloseableIterator<LogRecord> records(ReadContext context) {
            return loadFullBatch().records(context);
        }

        @Override
        public boolean isValid() {
            return loadFullBatch().isValid();
        }

        @Override
        public void ensureValid() {
            loadFullBatch().ensureValid();
        }

        @Override
        public int sizeInBytes() {
            return LOG_OVERHEAD + batchSize;
        }

        private DefaultLogRecordBatch toMemoryRecordBatch(ByteBuffer buffer) {
            DefaultLogRecordBatch records = new DefaultLogRecordBatch();
            records.pointTo(MemorySegment.wrap(buffer.array()), 0);
            return records;
        }

        private int headerSize() {
            return recordBatchHeaderSize(magic);
        }

        protected LogRecordBatch loadFullBatch() {
            if (fullBatch == null) {
                batchHeader = null;
                fullBatch = loadBatchWithSize(sizeInBytes(), true, "full record batch");
            }
            return fullBatch;
        }

        protected DefaultLogRecordBatch loadBatchHeader() {
            if (fullBatch != null) {
                return fullBatch;
            }

            if (batchHeader == null) {
                batchHeader = loadBatchWithSize(headerSize(), false, "record batch header");
            }

            return batchHeader;
        }

        protected ByteBuffer loadByteBufferWithSize(int size, int position, String description) {
            FileChannel channel = fileRecords.channel();
            try {
                return FileUtils.loadByteBufferFromFile(channel, size, position, description);
            } catch (IOException e) {
                throw new FlussRuntimeException(
                        "Failed to load record batch at position "
                                + position
                                + " from "
                                + fileRecords,
                        e);
            }
        }

        private DefaultLogRecordBatch loadBatchWithSize(
                int size, boolean isFull, String description) {
            ByteBuffer buffer = loadByteBufferWithSize(size, position, description);
            if (!isFull) {
                cachedHeaderBuffer = buffer;
            }
            return toMemoryRecordBatch(buffer);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FileChannelLogRecordBatch that = (FileChannelLogRecordBatch) o;

            FileChannel channel = fileRecords == null ? null : fileRecords.channel();
            FileChannel thatChannel = that.fileRecords == null ? null : that.fileRecords.channel();

            return offset == that.offset
                    && position == that.position
                    && batchSize == that.batchSize
                    && Objects.equals(channel, thatChannel);
        }

        @Override
        public int hashCode() {
            FileChannel channel = fileRecords == null ? null : fileRecords.channel();

            int result = Long.hashCode(offset);
            result = 31 * result + (channel != null ? channel.hashCode() : 0);
            result = 31 * result + position;
            result = 31 * result + batchSize;
            return result;
        }

        @Override
        public String toString() {
            return "FileChannelLogRecordBatch(magic: "
                    + magic
                    + ", offset: "
                    + offset
                    + ", size: "
                    + batchSize
                    + ")";
        }

        @Override
        public Optional<LogRecordBatchStatistics> getStatistics(ReadContext context) {
            if (context == null) {
                return Optional.empty();
            }

            // Return cached statistics if already parsed
            if (cachedStatistics != null) {
                return cachedStatistics;
            }

            if (magic < LogRecordBatchFormat.LOG_MAGIC_VALUE_V2) {
                // Statistics are only available in V2 and later
                cachedStatistics = Optional.empty();
                return cachedStatistics;
            }

            try {
                RowType rowType = context.getRowType(schemaId());
                if (rowType == null) {
                    cachedStatistics = Optional.empty();
                    return cachedStatistics;
                }

                DefaultLogRecordBatch header = loadBatchHeader();
                int statisticsOffset = header.getStatisticsOffset();
                int statisticsLength = header.sizeInBytes() - statisticsOffset;

                if (statisticsOffset == 0) {
                    cachedStatistics = Optional.empty();
                    return cachedStatistics;
                }

                ByteBuffer statisticsData =
                        loadByteBufferWithSize(
                                statisticsLength, position + statisticsOffset, "statistics");

                LogRecordBatchStatistics parsedStatistics =
                        LogRecordBatchStatisticsParser.parseStatistics(
                                statisticsData.array(), rowType, schemaId());
                cachedStatistics = Optional.ofNullable(parsedStatistics);
                return cachedStatistics;
            } catch (Exception e) {
                // If loading statistics fails, log the error and return empty
                LOG.error("Failed to load statistics for record batch at position {}", position, e);
                cachedStatistics = Optional.empty();
                return cachedStatistics;
            }
        }
    }
}
