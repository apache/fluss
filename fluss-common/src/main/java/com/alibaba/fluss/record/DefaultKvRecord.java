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
import com.alibaba.fluss.exception.InvalidRecordException;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.OutputView;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.compacted.CompactedRowWriter;
import com.alibaba.fluss.row.decode.RowDecoder;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.row.indexed.IndexedRowWriter;
import com.alibaba.fluss.utils.MurmurHashUtils;
import com.alibaba.fluss.utils.VarLengthUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is an immutable kv record. Different from {@link IndexedLogRecord}, it isn't designed
 * for persistence. The schema is as follows:
 *
 * <ul>
 *   <li>Length => int32
 *   <li>KeyLength => unsigned varint
 *   <li>Key => bytes
 *   <li>Row => {@link BinaryRow}
 * </ul>
 *
 * <p>When the row is null, no any byte will be remained after Key.
 *
 * @since 0.1
 */
@PublicEvolving
public class DefaultKvRecord implements KvRecord {

    static final int LENGTH_LENGTH = 4;
    private final RowDecoder rowDecoder;

    private MemorySegment segment;
    private int offset;
    private int sizeInBytes;

    private ByteBuffer key;
    private BinaryRow value;

    private DefaultKvRecord(RowDecoder rowDecoder) {
        this.rowDecoder = rowDecoder;
    }

    private void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
        this.segment = segment;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
        try {
            readKeyAndRow();
        } catch (IOException e) {
            throw new InvalidRecordException("Found invalid kv record structure.", e);
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
        DefaultKvRecord that = (DefaultKvRecord) o;
        return sizeInBytes == that.sizeInBytes
                && segment.equalTo(that.segment, offset, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, offset, sizeInBytes);
    }

    public static int writeTo(OutputView outputView, byte[] key, @Nullable BinaryRow row)
            throws IOException {
        // bytes for key length + bytes for key + bytes for row
        int sizeInBytes = sizeWithoutLength(key, row);

        // TODO using varint instead int to reduce storage size.
        // write record total bytes size.
        outputView.writeInt(sizeInBytes);

        // write key length, unsigned var int;
        VarLengthUtils.writeUnsignedVarInt(key.length, outputView);
        // write the real key
        outputView.write(key);

        if (row != null) {
            // write internal row, which is the value.
            serializeInternalRow(outputView, row);
        }
        return sizeInBytes + LENGTH_LENGTH;
    }

    public static KvRecord readFrom(
            MemorySegment segment,
            int position,
            short schemaId,
            KvRecordBatch.ReadContext readContext) {
        int sizeInBytes = segment.getInt(position);
        DefaultKvRecord kvRecord = new DefaultKvRecord(readContext.getRowDecoder(schemaId));
        kvRecord.pointTo(segment, position, sizeInBytes + LENGTH_LENGTH);
        return kvRecord;
    }

    /** Calculate the size of the kv record write to batch, including {@link #LENGTH_LENGTH}. */
    public static int sizeOf(byte[] key, @Nullable BinaryRow row) {
        return sizeWithoutLength(key, row) + LENGTH_LENGTH;
    }

    private static int sizeWithoutLength(byte[] key, @Nullable BinaryRow row) {
        return VarLengthUtils.sizeOfUnsignedVarInt(key.length)
                + key.length
                + (row == null ? 0 : row.getSizeInBytes());
    }

    private void readKeyAndRow() throws IOException {
        // start to read from the offset of key
        int currentOffset = offset + LENGTH_LENGTH;
        // now, read key;
        // read the length of key size
        int keyLength = VarLengthUtils.readUnsignedVarInt(segment, currentOffset);
        int bytesForKeyLength = VarLengthUtils.sizeOfUnsignedVarInt(keyLength);
        // seek to the position of real key
        currentOffset += bytesForKeyLength;
        key = segment.wrap(currentOffset, keyLength);

        // seek to the position of value
        currentOffset += keyLength;
        // now, read value;
        int nonValueLength = currentOffset - this.offset;
        int valueLength = sizeInBytes - nonValueLength;
        if (valueLength == 0) {
            value = null;
        } else {
            value = rowDecoder.decode(segment, currentOffset, valueLength);
        }
    }

    @Override
    public ByteBuffer getKey() {
        return key;
    }

    @Override
    public @Nullable BinaryRow getRow() {
        return value;
    }

    @Override
    public int getSizeInBytes() {
        return sizeInBytes;
    }

    private static void serializeInternalRow(OutputView outputView, InternalRow internalRow)
            throws IOException {
        if (internalRow instanceof IndexedRow) {
            IndexedRow indexedRow = (IndexedRow) internalRow;
            IndexedRowWriter.serializeIndexedRow(indexedRow, outputView);
        } else if (internalRow instanceof CompactedRow) {
            CompactedRow compactedRow = (CompactedRow) internalRow;
            CompactedRowWriter.serializeCompactedRow(compactedRow, outputView);
        } else {
            throw new IllegalArgumentException(
                    "No such internal row serializer for: "
                            + internalRow.getClass().getSimpleName());
        }
    }
}
