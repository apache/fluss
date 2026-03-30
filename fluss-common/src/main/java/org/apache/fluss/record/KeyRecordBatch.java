/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.record;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.KeyDecoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;

/**
 * A lazy-decoding KvRecordBatch that stores key bytes and converts them to KvRecords on-demand
 * during iteration.
 *
 * <p>When iterating via {@link #records(ReadContext)}, each key is decoded and a full row is
 * constructed with key fields populated and non-key fields set to null. This avoids upfront
 * full-batch decoding, improving memory efficiency.
 */
public class KeyRecordBatch implements KvRecordBatch {

    private final List<byte[]> keyBytes;
    private final short schemaId;
    private final KvFormat kvFormat;
    private final short kvFormatVersion;
    private final boolean defaultBucketKey;
    private final @Nullable DataLakeFormat lakeFormat;

    public static KeyRecordBatch create(List<byte[]> keyBytes, TableInfo tableInfo) {
        TableConfig tableConfig = tableInfo.getTableConfig();
        KvFormat kvFormat = tableConfig.getKvFormat();
        short kvFormatVersion = tableConfig.getKvFormatVersion().orElse(1).shortValue();
        short schemaId = (short) tableInfo.getSchemaId();
        boolean defaultBucketKey = tableInfo.isDefaultBucketKey();
        return new KeyRecordBatch(
                keyBytes,
                kvFormat,
                kvFormatVersion,
                schemaId,
                defaultBucketKey,
                tableConfig.getDataLakeFormat().orElse(null));
    }

    @VisibleForTesting
    KeyRecordBatch(List<byte[]> keyBytes, KvFormat kvFormat, short schemaId) {
        this(keyBytes, kvFormat, (short) 1, schemaId, true, null);
    }

    /**
     * Creates a KeyRecordBatch for lazy KvRecord construction.
     *
     * @param keyBytes the list of encoded key bytes
     */
    public KeyRecordBatch(
            List<byte[]> keyBytes,
            KvFormat kvFormat,
            short kvFormatVersion,
            short schemaId,
            boolean defaultBucketKey,
            @Nullable DataLakeFormat lakeFormat) {
        this.keyBytes = keyBytes;
        this.schemaId = schemaId;
        this.kvFormat = kvFormat;
        this.kvFormatVersion = kvFormatVersion;
        this.defaultBucketKey = defaultBucketKey;
        this.lakeFormat = lakeFormat;
    }

    @Override
    public Iterable<KvRecord> records(ReadContext readContext) {
        return () -> new LazyKvRecordIterator(readContext);
    }

    @Override
    public int getRecordCount() {
        return keyBytes.size();
    }

    @Override
    public short schemaId() {
        return schemaId;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public void ensureValid() {
        // In-memory batch is always valid
    }

    @Override
    public long checksum() {
        return 0;
    }

    @Override
    public byte magic() {
        return CURRENT_KV_MAGIC_VALUE;
    }

    @Override
    public long writerId() {
        return NO_WRITER_ID;
    }

    @Override
    public int batchSequence() {
        return NO_BATCH_SEQUENCE;
    }

    @Override
    public int sizeInBytes() {
        return keyBytes.stream().mapToInt(k -> k.length).sum();
    }

    /** Lazy iterator that constructs KvRecord on-demand during iteration. */
    private class LazyKvRecordIterator implements Iterator<KvRecord> {
        private int index = 0;

        private final int fieldCount;
        private final int[] keyIdxByFieldIdx; // Maps field index to key index (-1 if not a key)
        private final InternalRow.FieldGetter[] keyFieldGetters;
        private final RowEncoder rowEncoder;
        private final KeyDecoder keyDecoder;

        LazyKvRecordIterator(ReadContext context) {
            SchemaGetter schemaGetter = context.getSchemaGetter();
            Schema schema = schemaGetter.getSchema(schemaId);
            RowType rowType = schema.getRowType();
            List<String> keyColumnNames = schema.getPrimaryKeyColumnNames();

            this.fieldCount = rowType.getFieldCount();
            int[] keyFieldIndexes = schema.getPrimaryKeyIndexes();
            this.keyDecoder =
                    KeyDecoder.ofPrimaryKeyDecoder(
                            rowType, keyColumnNames, kvFormatVersion, lakeFormat, defaultBucketKey);
            this.keyFieldGetters = new InternalRow.FieldGetter[keyColumnNames.size()];

            // Build reverse mapping: field index -> key index
            this.keyIdxByFieldIdx = new int[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                keyIdxByFieldIdx[i] = -1;
            }
            for (int keyIdx = 0; keyIdx < keyFieldIndexes.length; keyIdx++) {
                keyIdxByFieldIdx[keyFieldIndexes[keyIdx]] = keyIdx;
            }

            // Create field getters for key fields (reading from decoded key row at position i)
            for (int i = 0; i < keyColumnNames.size(); i++) {
                DataType keyFieldType = rowType.getTypeAt(keyFieldIndexes[i]);
                keyFieldGetters[i] = InternalRow.createFieldGetter(keyFieldType, i);
            }

            this.rowEncoder = RowEncoder.create(kvFormat, rowType);
        }

        @Override
        public boolean hasNext() {
            return index < keyBytes.size();
        }

        @Override
        public KvRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] key = keyBytes.get(index++);

            InternalRow keyRow = keyDecoder.decodeKey(key);

            // Build full row: key fields from decoded values, non-key fields as null
            rowEncoder.startNewRow();
            for (int i = 0; i < fieldCount; i++) {
                int keyIdx = keyIdxByFieldIdx[i];
                if (keyIdx != -1) {
                    // This is a key field - copy from decoded key row
                    Object value = keyFieldGetters[keyIdx].getFieldOrNull(keyRow);
                    rowEncoder.encodeField(i, value);
                } else {
                    // Non-key field - set to null
                    rowEncoder.encodeField(i, null);
                }
            }
            BinaryRow binaryRow = rowEncoder.finishRow();

            return new KeyOnlyKvRecord(key, binaryRow);
        }
    }

    /** Simple KvRecord implementation for key-only records. */
    private static class KeyOnlyKvRecord implements KvRecord {
        private final byte[] key;
        private final BinaryRow row;

        KeyOnlyKvRecord(byte[] key, BinaryRow row) {
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
            return key.length + (row != null ? row.getSizeInBytes() : 0);
        }
    }
}
