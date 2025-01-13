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

import com.alibaba.fluss.memory.UnmanagedPagedOutputView;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.BytesUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.fluss.record.LogRecordBatch.NO_BATCH_SEQUENCE;
import static com.alibaba.fluss.record.LogRecordBatch.NO_WRITER_ID;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;

/** Test utils for kv record. */
public class KvRecordTestUtils {

    private KvRecordTestUtils() {}

    /** A factory to create {@link KvRecordBatch}. */
    public static class KvRecordBatchFactory {

        private final int schemaId;

        KvRecordBatchFactory(int schemaId) {
            this.schemaId = schemaId;
        }

        public static KvRecordBatchFactory of(int schemaId) {
            return new KvRecordBatchFactory(schemaId);
        }

        public KvRecordBatch ofRecords(KvRecord... kvRecord) throws IOException {
            return ofRecords(Arrays.asList(kvRecord));
        }

        public KvRecordBatch ofRecords(List<KvRecord> records) throws IOException {
            return ofRecords(records, NO_WRITER_ID, NO_BATCH_SEQUENCE);
        }

        public KvRecordBatch ofRecords(
                List<KvRecord> records, long writeClientId, int batchSequenceId)
                throws IOException {
            KvRecordBatchBuilder builder =
                    KvRecordBatchBuilder.builder(
                            schemaId,
                            Integer.MAX_VALUE,
                            new UnmanagedPagedOutputView(100),
                            KvFormat.COMPACTED);
            for (KvRecord kvRecord : records) {
                builder.append(BytesUtils.toArray(kvRecord.getKey()), kvRecord.getRow());
            }

            builder.setWriterState(writeClientId, batchSequenceId);
            KvRecordBatch kvRecords = DefaultKvRecordBatch.pointToBytesView(builder.build());
            kvRecords.ensureValid();
            builder.close();
            return kvRecords;
        }
    }

    /** A factory to create {@link KvRecord} whose key and value is specified by user. */
    public static class KvRecordFactory {
        private final RowType rowType;

        private KvRecordFactory(RowType rowType) {
            this.rowType = rowType;
        }

        public static KvRecordFactory of(RowType rowType) {
            return new KvRecordFactory(rowType);
        }

        /**
         * Create a KvRecord with give key and value. If the given value is null, it means to create
         * a kv record for deletion.
         */
        public KvRecord ofRecord(byte[] key, @Nullable Object[] value) {
            if (value == null) {
                return new SimpleTestKvRecord(key, null);
            } else {
                return new SimpleTestKvRecord(key, compactedRow(rowType, value));
            }
        }

        /**
         * Create a KvRecord with give key and value. If the given value is null, it means to create
         * a kv record for deletion.
         */
        public KvRecord ofRecord(String key, @Nullable Object[] value) {
            return ofRecord(key.getBytes(), value);
        }
    }

    /**
     * A factory to create {@link KvRecord} whose key is extract from the values according to the
     * primary key index.
     */
    public static class PKBasedKvRecordFactory {
        private final RowType rowType;

        private final KeyEncoder keyEncoder;

        private PKBasedKvRecordFactory(RowType rowType, int[] pkIndex) {
            this.rowType = rowType;
            this.keyEncoder = new KeyEncoder(rowType, pkIndex);
        }

        public static PKBasedKvRecordFactory of(RowType rowType, int[] pkIndex) {
            return new PKBasedKvRecordFactory(rowType, pkIndex);
        }

        /**
         * Create a KvRecord with given value. The key will be extracted from the primary key of the
         * IndexedRow constructed by given value.
         */
        public KvRecord ofRecord(@Nonnull Object[] value) {
            CompactedRow row = compactedRow(rowType, value);
            return new SimpleTestKvRecord(keyEncoder.encode(row), row);
        }
    }

    private static class SimpleTestKvRecord implements KvRecord {
        private final byte[] key;
        private final BinaryRow row;

        SimpleTestKvRecord(byte[] key, @Nullable BinaryRow row) {
            this.key = key;
            this.row = row;
        }

        @Override
        public ByteBuffer getKey() {
            return ByteBuffer.wrap(key);
        }

        @Override
        public BinaryRow getRow() {
            return row;
        }

        @Override
        public int getSizeInBytes() {
            throw new UnsupportedOperationException();
        }
    }
}
