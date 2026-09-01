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

package org.apache.fluss.row.encode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.UnsafeUtils;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Physical layout of raw KV value bytes.
 *
 * <p>PLAIN (version 1): {@code [little-endian schema id][BinaryRow payload]}.
 *
 * <p>TAGGED (version 2): {@code [big-endian long tag][little-endian schema id][BinaryRow payload]}.
 * The tag is an opaque signed 64-bit value whose semantics are defined by the producer and consumer
 * of the selected layout. Its byte order follows FRocksDB's native {@code DeserializeTimestamp}
 * contract, which reads the eight-byte value most-significant byte first.
 */
@Internal
public final class KvValueLayout {

    private static final int SCHEMA_ID_LENGTH = 2;
    private static final int VALUE_TAG_LENGTH = 8;
    private static final int NO_OFFSET = -1;

    public static final KvValueLayout PLAIN = new KvValueLayout(1, 0, 0, 2, NO_OFFSET);
    public static final KvValueLayout TAGGED = new KvValueLayout(2, 8, 8, 10, 0);

    private final int version;
    private final int valueBodyOffset;
    private final int schemaIdOffset;
    private final int rowPayloadOffset;
    private final int valueTagOffset;

    private KvValueLayout(
            int version,
            int valueBodyOffset,
            int schemaIdOffset,
            int rowPayloadOffset,
            int valueTagOffset) {
        this.version = version;
        this.valueBodyOffset = valueBodyOffset;
        this.schemaIdOffset = schemaIdOffset;
        this.rowPayloadOffset = rowPayloadOffset;
        this.valueTagOffset = valueTagOffset;
    }

    /** Returns the layout for the persisted version. */
    public static KvValueLayout fromVersion(int version) {
        switch (version) {
            case 1:
                return PLAIN;
            case 2:
                return TAGGED;
            default:
                throw new IllegalArgumentException(
                        "Unsupported KV value layout version " + version + ".");
        }
    }

    /** Returns the layout persisted in the table config, defaulting to the plain layout. */
    public static KvValueLayout fromTableConfig(TableConfig tableConfig) {
        return fromVersion(tableConfig.getKvValueLayoutVersion().orElse(PLAIN.version()));
    }

    /** Returns the persisted version of this layout. */
    public int version() {
        return version;
    }

    /** Returns the byte offset of the RPC value body in a raw KV value. */
    public int valueBodyOffset() {
        return valueBodyOffset;
    }

    /** Returns the RPC value body length for a raw KV value length. */
    public int valueBodyLength(int valueLength) {
        return valueLength - valueBodyOffset;
    }

    /** Returns the RPC value body as a zero-copy slice of a raw KV value. */
    public @Nullable ByteArraySlice toValueBodySlice(@Nullable byte[] value) {
        if (value == null) {
            return null;
        }
        return ByteArraySlice.wrap(value, valueBodyOffset(), valueBodyLength(value.length));
    }

    /** Returns the byte offset of schema id in a raw KV value. */
    public int schemaIdOffset() {
        return schemaIdOffset;
    }

    /** Returns the encoded schema id length in bytes. */
    public int schemaIdLength() {
        return SCHEMA_ID_LENGTH;
    }

    /** Returns whether the raw KV value has an internal value tag. */
    public boolean hasValueTag() {
        return valueTagOffset != NO_OFFSET;
    }

    /** Returns the byte offset of the internal value tag. */
    public int valueTagOffset() {
        checkState(hasValueTag(), "KV value layout does not have a value tag.");
        return valueTagOffset;
    }

    /** Returns the internal value tag length in bytes. */
    public int valueTagLength() {
        return hasValueTag() ? VALUE_TAG_LENGTH : 0;
    }

    /** Returns the byte offset of row payload in a raw KV value. */
    public int rowPayloadOffset() {
        return rowPayloadOffset;
    }

    /** Returns the row payload length for a raw KV value length. */
    public int rowPayloadLength(int valueLength) {
        if (valueLength < rowPayloadOffset) {
            throw new IllegalArgumentException(
                    "valueLength must be at least row payload offset "
                            + rowPayloadOffset
                            + ", but was "
                            + valueLength
                            + ".");
        }
        return valueLength - rowPayloadOffset;
    }

    /** Reads the schema id from a raw KV value. */
    public short readSchemaId(MemorySegment value) {
        return readSchemaId(value, 0);
    }

    /** Reads the schema id from a raw KV value embedded at the given offset. */
    public short readSchemaId(MemorySegment value, int valueOffset) {
        return value.getShort(valueOffset + schemaIdOffset);
    }

    /** Writes the schema id to a raw KV value. */
    public void writeSchemaId(byte[] value, short schemaId) {
        writeSchemaId(value, 0, schemaId);
    }

    /** Writes the schema id to a raw KV value embedded at the given offset. */
    public void writeSchemaId(byte[] value, int valueOffset, short schemaId) {
        short littleEndianSchemaId =
                MemorySegment.LITTLE_ENDIAN ? schemaId : Short.reverseBytes(schemaId);
        UnsafeUtils.putShort(value, valueOffset + schemaIdOffset, littleEndianSchemaId);
    }

    /**
     * Reads the opaque signed 64-bit value tag from a raw KV value. The layout does not assign
     * semantics to this field.
     */
    public long readValueTag(MemorySegment value) {
        return readValueTag(value, 0);
    }

    /**
     * Reads the opaque signed 64-bit value tag from a raw KV value embedded at the given offset.
     * The layout does not assign semantics to this field.
     */
    public long readValueTag(MemorySegment value, int valueOffset) {
        return value.getLongBigEndian(valueOffset + valueTagOffset());
    }

    /**
     * Writes an opaque signed 64-bit value tag to a raw KV value. The producer defines the tag's
     * semantics.
     */
    public void writeValueTag(byte[] value, long valueTag) {
        writeValueTag(value, 0, valueTag);
    }

    /**
     * Writes an opaque signed 64-bit value tag to a raw KV value embedded at the given offset. The
     * producer defines the tag's semantics.
     */
    public void writeValueTag(byte[] value, int valueOffset, long valueTag) {
        int absoluteValueTagOffset = valueOffset + valueTagOffset();
        if (absoluteValueTagOffset < 0
                || absoluteValueTagOffset > value.length - VALUE_TAG_LENGTH) {
            throw new IndexOutOfBoundsException(
                    "Cannot write value tag at offset "
                            + absoluteValueTagOffset
                            + " to value of length "
                            + value.length
                            + ".");
        }
        long bigEndianValueTag =
                MemorySegment.LITTLE_ENDIAN ? Long.reverseBytes(valueTag) : valueTag;
        UnsafeUtils.putLong(value, absoluteValueTagOffset, bigEndianValueTag);
    }
}
