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
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * KvRecordBatch implementation for magic 0 and above. The schema of {@link ValueRecordBatch} is
 * given below:
 *
 * <ul>
 *   ValueRecordBatch =>
 *   <li>Length => Int32
 *   <li>Magic => Int8
 *   <li>RecordCount => Int32
 *   <li>Records => [ValueRecord]
 * </ul>
 */
public class DefaultValueRecordBatch implements ValueRecordBatch {

    static final int LENGTH_OFFSET = 0;
    static final int LENGTH_LENGTH = 4;
    static final int MAGIC_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH;
    static final int MAGIC_LENGTH = 1;
    static final int RECORDS_COUNT_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    static final int RECORD_COUNT_LENGTH = 4;
    static final int RECORDS_OFFSET = LENGTH_LENGTH + MAGIC_LENGTH + RECORD_COUNT_LENGTH;
    static final int RECORD_BATCH_HEADER_SIZE = RECORDS_OFFSET;
    static final int BATCH_OVERHEAD = LENGTH_OFFSET + LENGTH_LENGTH;

    private MemorySegment segment;
    private int position;

    public void pointTo(MemorySegment segment, int position) {
        this.segment = segment;
        this.position = position;
    }

    public MemorySegment getSegment() {
        return segment;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public int sizeInBytes() {
        return BATCH_OVERHEAD + segment.getInt(position + LENGTH_OFFSET);
    }

    @Override
    public byte magic() {
        return segment.get(position + MAGIC_OFFSET);
    }

    @Override
    public int getRecordCount() {
        return segment.getInt(position + RECORDS_COUNT_OFFSET);
    }

    @Override
    public Iterable<ValueRecord> records(ReadContext readContext) {
        return () -> iterator(readContext);
    }

    private Iterator<ValueRecord> iterator(ReadContext readContext) {
        if (getRecordCount() == 0) {
            return Collections.emptyIterator();
        }

        return new ValueRecordIterator() {
            int position = DefaultValueRecordBatch.this.position + RECORD_BATCH_HEADER_SIZE;
            int iteratorNumber = 0;

            @Override
            protected ValueRecord readNext() {
                ValueRecord valueRecord =
                        DefaultValueRecord.readFrom(segment, position, readContext);
                iteratorNumber++;
                position += valueRecord.getSizeInBytes();
                return valueRecord;
            }

            @Override
            protected boolean ensureNoneRemaining() {
                return true;
            }

            @Override
            public void close() {
                // do nothing
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultValueRecordBatch that = (DefaultValueRecordBatch) o;
        int sizeInBytes = sizeInBytes();
        return sizeInBytes == that.sizeInBytes()
                && segment.equalTo(that.segment, position, that.position, sizeInBytes);
    }

    // ------------------------------------------------------------------------------------------

    abstract class ValueRecordIterator implements CloseableIterator<ValueRecord> {
        private final int numRecords;
        private int readRecords = 0;

        ValueRecordIterator() {
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
        public ValueRecord next() {
            if (readRecords >= numRecords) {
                throw new NoSuchElementException();
            }
            readRecords++;
            ValueRecord rec = readNext();

            if (readRecords == numRecords) {
                // Validate that the actual size of the batch is equal to declared size
                // by checking that after reading declared number of items, there no items left
                // (overflow case, i.e. reading past buffer end is checked elsewhere).
                if (!ensureNoneRemaining()) {
                    throw new IllegalArgumentException("Incorrect declared batch size");
                }
            }
            return rec;
        }

        protected abstract ValueRecord readNext();

        protected abstract boolean ensureNoneRemaining();

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /** Make a {@link DefaultValueRecordBatch} instance from the given bytes. */
    public static DefaultValueRecordBatch pointToBytes(byte[] bytes) {
        return pointToBytes(bytes, 0);
    }

    /** Make a {@link DefaultKvRecordBatch} instance from the given bytes. */
    public static DefaultValueRecordBatch pointToBytes(byte[] bytes, int offset) {
        DefaultValueRecordBatch records = new DefaultValueRecordBatch();
        records.pointTo(MemorySegment.wrap(bytes), offset);
        return records;
    }

    /** Make a {@link DefaultValueRecordBatch} instance from the given memory segment. */
    public static DefaultValueRecordBatch pointToMemory(MemorySegment segment, int position) {
        DefaultValueRecordBatch records = new DefaultValueRecordBatch();
        records.pointTo(segment, position);
        return records;
    }

    /**
     * Make a {@link DefaultValueRecordBatch} instance from the given byte buffer. The byte buffer
     * can be either direct or non-direct.
     */
    public static DefaultValueRecordBatch pointToByteBuffer(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            MemorySegment segment = MemorySegment.wrapOffHeapMemory(buffer);
            return pointToMemory(segment, buffer.position());
        } else if (buffer.hasArray()) {
            byte[] bytes = buffer.array();
            int offset = buffer.arrayOffset() + buffer.position();
            return pointToBytes(bytes, offset);
        } else {
            // fallback to copy bytes
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return pointToBytes(bytes);
        }
    }

    public static Builder builder() throws IOException {
        return new Builder(CURRENT_VALUE_BATCH_MAGIC, new MemorySegmentOutputView(1024));
    }

    /** Builder for {@link DefaultValueRecordBatch}. */
    public static class Builder {

        private final byte magic;
        private final MemorySegmentOutputView outputView;
        private int currentRecordNumber;
        private int sizeInBytes;
        private boolean isClosed;
        private DefaultValueRecordBatch builtRecords;

        Builder(byte magic, MemorySegmentOutputView outputView) throws IOException {
            this.magic = magic;
            this.outputView = outputView;
            // We don't need to write header information while the builder creating, we'll skip it
            // first.
            outputView.setPosition(RECORD_BATCH_HEADER_SIZE);
            this.sizeInBytes = RECORD_BATCH_HEADER_SIZE;
        }

        public void append(short schemaId, BinaryRow row) throws IOException {
            if (isClosed) {
                throw new IllegalStateException(
                        "Tried to add a record, but ValueRecordBatchBuilder is closed for record adds.");
            }
            DefaultValueRecord record = new DefaultValueRecord(schemaId, row);
            int recordByteSize = record.writeTo(outputView);
            sizeInBytes += recordByteSize;
            currentRecordNumber++;
        }

        /**
         * @param valueBytes consisted of schema id and the row encoded in the value bytes
         */
        public void append(byte[] valueBytes) throws IOException {
            if (isClosed) {
                throw new IllegalStateException(
                        "Tried to add a record, but ValueRecordBatchBuilder is closed for record adds.");
            }
            outputView.writeInt(valueBytes.length);
            outputView.write(valueBytes);
            sizeInBytes += valueBytes.length + LENGTH_LENGTH;
            currentRecordNumber++;
        }

        public DefaultValueRecordBatch build() throws IOException {
            writeBatchHeader();
            MemorySegment segment = outputView.getMemorySegment();
            return DefaultValueRecordBatch.pointToMemory(segment, 0);
        }

        private void writeBatchHeader() throws IOException {
            outputView.setPosition(0);
            // update header
            outputView.writeInt(sizeInBytes - LENGTH_LENGTH);
            outputView.writeByte(magic);
            outputView.writeInt(currentRecordNumber);
            // reset the position to origin position.
            outputView.setPosition(sizeInBytes);
        }

        public void close() throws Exception {
            isClosed = true;
        }
    }
}
