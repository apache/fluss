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
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.utils.UnsafeUtils;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Versioned physical layout of raw KV value bytes. */
@Internal
public final class KvValueLayout {

    private static final int MIN_KV_FORMAT_VERSION = 1;
    private static final int SCHEMA_ID_OFFSET = 0;
    private static final int SCHEMA_ID_LENGTH = 2;
    private static final int VALUE_TAG_OFFSET = SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH;
    private static final int VALUE_TAG_LENGTH = 8;
    private static final int NO_OFFSET = -1;
    private static final KvValueLayout KV_FORMAT_VERSION_1_LAYOUT =
            new KvValueLayout(
                    HeaderKind.SCHEMA_ONLY, SCHEMA_ID_OFFSET, SCHEMA_ID_LENGTH, NO_OFFSET);
    private static final KvValueLayout KV_FORMAT_VERSION_2_LAYOUT =
            new KvValueLayout(
                    HeaderKind.SCHEMA_ONLY, SCHEMA_ID_OFFSET, SCHEMA_ID_LENGTH, NO_OFFSET);
    private static final KvValueLayout KV_FORMAT_VERSION_3_LAYOUT =
            new KvValueLayout(
                    HeaderKind.SCHEMA_WITH_VALUE_TAG,
                    SCHEMA_ID_OFFSET,
                    SCHEMA_ID_LENGTH + VALUE_TAG_LENGTH,
                    VALUE_TAG_OFFSET);

    private final HeaderKind headerKind;
    private final int schemaIdOffset;
    private final int rowPayloadOffset;
    private final int valueTagOffset;

    private KvValueLayout(
            HeaderKind headerKind, int schemaIdOffset, int rowPayloadOffset, int valueTagOffset) {
        this.headerKind = headerKind;
        this.schemaIdOffset = schemaIdOffset;
        this.rowPayloadOffset = rowPayloadOffset;
        this.valueTagOffset = valueTagOffset;
    }

    /** Returns the KV value layout for the table KV format version. */
    public static KvValueLayout forKvFormatVersion(int kvFormatVersion) {
        switch (kvFormatVersion) {
            case MIN_KV_FORMAT_VERSION:
                return KV_FORMAT_VERSION_1_LAYOUT;
            case KV_FORMAT_VERSION_2:
                return KV_FORMAT_VERSION_2_LAYOUT;
            case KV_FORMAT_VERSION_3:
                return KV_FORMAT_VERSION_3_LAYOUT;
            default:
                throw new IllegalArgumentException(
                        "Unsupported KV format version " + kvFormatVersion + ".");
        }
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

    HeaderKind headerKind() {
        return headerKind;
    }

    /** Reads the schema id from a raw KV value. */
    public short readSchemaId(MemorySegment value) {
        return readSchemaId(value, 0);
    }

    /** Reads the schema id from a raw KV value embedded at the given offset. */
    public short readSchemaId(MemorySegment value, int valueOffset) {
        return value.getShort(valueOffset + schemaIdOffset());
    }

    /** Writes the schema id to a raw KV value. */
    public void writeSchemaId(byte[] value, short schemaId) {
        writeSchemaId(value, 0, schemaId);
    }

    /** Writes the schema id to a raw KV value embedded at the given offset. */
    public void writeSchemaId(byte[] value, int valueOffset, short schemaId) {
        UnsafeUtils.putShort(value, valueOffset + schemaIdOffset(), schemaId);
    }

    /** Reads the internal value tag from a raw KV value. */
    public long readValueTag(MemorySegment value) {
        return readValueTag(value, 0);
    }

    /** Reads the internal value tag from a raw KV value embedded at the given offset. */
    public long readValueTag(MemorySegment value, int valueOffset) {
        return value.getLongBigEndian(valueOffset + valueTagOffset());
    }

    /** Writes the internal value tag to a raw KV value. */
    public void writeValueTag(byte[] value, long valueTag) {
        writeValueTag(value, 0, valueTag);
    }

    /** Writes the internal value tag to a raw KV value embedded at the given offset. */
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

    enum HeaderKind {
        SCHEMA_ONLY,
        SCHEMA_WITH_VALUE_TAG
    }
}
