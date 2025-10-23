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
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.FileUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Objects;

import static org.apache.fluss.record.LogRecordBatchFormat.BASE_OFFSET_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A log input stream which is backed by a {@link FileChannel}. */
public class FileLogInputStream
        implements LogInputStream<FileLogInputStream.FileChannelLogRecordBatch> {
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

        private LogRecordBatch fullBatch;
        private LogRecordBatch batchHeader;

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

        private LogRecordBatch toMemoryRecordBatch(ByteBuffer buffer) {
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
                fullBatch = loadBatchWithSize(sizeInBytes(), "full record batch");
            }
            return fullBatch;
        }

        protected LogRecordBatch loadBatchHeader() {
            if (fullBatch != null) {
                return fullBatch;
            }

            if (batchHeader == null) {
                batchHeader = loadBatchWithSize(headerSize(), "record batch header");
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

        private LogRecordBatch loadBatchWithSize(int size, String description) {
            return toMemoryRecordBatch(loadByteBufferWithSize(size, position, description));
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
    }
}
