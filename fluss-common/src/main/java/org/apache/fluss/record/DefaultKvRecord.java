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
import org.apache.fluss.exception.InvalidRecordException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.utils.MurmurHashUtils;
import org.apache.fluss.utils.VarLengthUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is an immutable kv record. Different from {@link IndexedLogRecord}, it isn't designed
 * for persistence. The layout without an explicit mutation type is as follows:
 *
 * <ul>
 *   <li>Length => int32
 *   <li>KeyLength => unsigned varint
 *   <li>Key => bytes
 *   <li>Row => {@link BinaryRow}
 * </ul>
 *
 * <p>With the batch mutation type attribute set, a MutationType byte follows Length:
 *
 * <ul>
 *   <li>Length => int32
 *   <li>MutationType => int8 (see {@link MutationType})
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

    /** Size of the explicit MutationType byte. */
    static final int MUTATION_TYPE_LENGTH = 1;

    private final RowDecoder rowDecoder;

    private MemorySegment segment;
    private int offset;
    private int sizeInBytes;

    private ByteBuffer key;
    private BinaryRow value;

    // Records without an explicit mutation type infer it from row presence.
    @Nullable private MutationType mutationType;

    /**
     * Minimum record body size with a mutation type: 1 byte MutationType + 1 byte varint key
     * length.
     */
    static final int MIN_RECORD_SIZE_WITH_MUTATION_TYPE = MUTATION_TYPE_LENGTH + 1;

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
        int sizeInBytes = sizeWithoutLength(key, row);

        // TODO using varint instead int to reduce storage size.
        outputView.writeInt(sizeInBytes);
        VarLengthUtils.writeUnsignedVarInt(key.length, outputView);
        outputView.write(key);

        if (row != null) {
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

    /**
     * Write a KvRecord carrying an explicit mutation type to the output view.
     *
     * <ul>
     *   <li>Length => int32 (covers MutationType + KeyLength + Key + Row)
     *   <li>MutationType => int8
     *   <li>KeyLength => unsigned varint
     *   <li>Key => bytes
     *   <li>Row => {@link BinaryRow} (absent when row is null)
     * </ul>
     */
    public static int writeToWithMutationType(
            OutputView outputView, MutationType mutationType, byte[] key, @Nullable BinaryRow row)
            throws IOException {
        int sizeInBytes = sizeWithoutLengthWithMutationType(key, row);

        outputView.writeInt(sizeInBytes);
        outputView.writeByte(mutationType.getValue());
        VarLengthUtils.writeUnsignedVarInt(key.length, outputView);
        outputView.write(key);

        if (row != null) {
            serializeInternalRow(outputView, row);
        }
        return sizeInBytes + LENGTH_LENGTH;
    }

    /**
     * Read a KvRecord carrying an explicit mutation type from the given memory segment. The
     * MutationType byte is read and stored explicitly. Must set mutationType BEFORE pointTo so that
     * readKeyAndRow knows to skip the extra byte.
     */
    public static KvRecord readFromWithMutationType(
            MemorySegment segment,
            int position,
            short schemaId,
            KvRecordBatch.ReadContext readContext) {
        int sizeInBytes = segment.getInt(position);
        if (sizeInBytes < MIN_RECORD_SIZE_WITH_MUTATION_TYPE) {
            throw new InvalidRecordException(
                    "Invalid KvRecord size with mutation type: "
                            + sizeInBytes
                            + " at position "
                            + position
                            + ". Minimum size is "
                            + MIN_RECORD_SIZE_WITH_MUTATION_TYPE
                            + " bytes.");
        }
        DefaultKvRecord kvRecord = new DefaultKvRecord(readContext.getRowDecoder(schemaId));
        // Read the mutation type byte before pointTo, so readKeyAndRow can skip it.
        byte mutationByte = segment.get(position + LENGTH_LENGTH);
        try {
            kvRecord.mutationType = MutationType.fromValue(mutationByte);
        } catch (IllegalArgumentException e) {
            throw new InvalidRecordException(
                    "Invalid MutationType byte 0x"
                            + Integer.toHexString(mutationByte & 0xFF)
                            + " at position "
                            + (position + LENGTH_LENGTH)
                            + ".",
                    e);
        }
        kvRecord.pointTo(segment, position, sizeInBytes + LENGTH_LENGTH);
        return kvRecord;
    }

    /**
     * Calculate the size of a kv record carrying a mutation type, including {@link #LENGTH_LENGTH}.
     * This is {@link #sizeOf(byte[], BinaryRow)} + 1 byte for MutationType.
     */
    public static int sizeOfWithMutationType(byte[] key, @Nullable BinaryRow row) {
        return sizeWithoutLengthWithMutationType(key, row) + LENGTH_LENGTH;
    }

    private static int sizeWithoutLengthWithMutationType(byte[] key, @Nullable BinaryRow row) {
        return MUTATION_TYPE_LENGTH + sizeWithoutLength(key, row);
    }

    private static int sizeWithoutLength(byte[] key, @Nullable BinaryRow row) {
        return VarLengthUtils.sizeOfUnsignedVarInt(key.length)
                + key.length
                + (row == null ? 0 : row.getSizeInBytes());
    }

    private void readKeyAndRow() throws IOException {
        int currentOffset = offset + LENGTH_LENGTH;
        // Skip the explicit mutation type byte, if present (already read before pointTo).
        if (mutationType != null) {
            currentOffset += MUTATION_TYPE_LENGTH;
        }
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
    public MutationType getMutationType() {
        if (mutationType != null) {
            return mutationType;
        }
        // Infer the mutation type from row presence when it is not explicit.
        return value == null ? MutationType.DELETE : MutationType.UPSERT;
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
