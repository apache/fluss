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

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.apache.fluss.config.ConfigOptions.MAX_KV_FORMAT_VERSION;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Versioned layout of the raw value bytes stored in KV storage. */
@Internal
public final class ValueLayout {

    private static final int MIN_KV_FORMAT_VERSION = 1;
    private static final int SCHEMA_ID_OFFSET = 0;
    private static final int SCHEMA_ID_LENGTH = 2;
    private static final int VALUE_TIMESTAMP_OFFSET = SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH;
    private static final int VALUE_TIMESTAMP_LENGTH = 8;

    private final boolean hasValueTimestamp;

    private ValueLayout(boolean hasValueTimestamp) {
        this.hasValueTimestamp = hasValueTimestamp;
    }

    /** Returns the value layout for the table KV format version. */
    public static ValueLayout forVersion(int kvFormatVersion) {
        checkArgument(
                kvFormatVersion >= MIN_KV_FORMAT_VERSION,
                "kvFormatVersion must be at least %s, but was %s.",
                MIN_KV_FORMAT_VERSION,
                kvFormatVersion);
        checkArgument(
                kvFormatVersion <= MAX_KV_FORMAT_VERSION,
                "kvFormatVersion must not exceed %s, but was %s.",
                MAX_KV_FORMAT_VERSION,
                kvFormatVersion);
        return new ValueLayout(kvFormatVersion >= KV_FORMAT_VERSION_3);
    }

    /** Returns the byte offset of schema id in a raw value. */
    public int schemaIdOffset() {
        return SCHEMA_ID_OFFSET;
    }

    /** Returns the encoded schema id length in bytes. */
    public int schemaIdLength() {
        return SCHEMA_ID_LENGTH;
    }

    /** Returns whether the raw value has an internal value timestamp prefix. */
    public boolean hasValueTimestamp() {
        return hasValueTimestamp;
    }

    /** Returns the byte offset of the internal value timestamp. */
    public int valueTimestampOffset() {
        checkState(hasValueTimestamp, "Value layout does not have a value timestamp.");
        return VALUE_TIMESTAMP_OFFSET;
    }

    /** Returns the internal value timestamp length in bytes. */
    public int valueTimestampLength() {
        return hasValueTimestamp ? VALUE_TIMESTAMP_LENGTH : 0;
    }

    /** Returns the byte offset of row payload in a raw value. */
    public int rowOffset() {
        return SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH + valueTimestampLength();
    }

    /** Returns the row payload length for a raw value length. */
    public int rowLength(int valueLength) {
        int rowOffset = rowOffset();
        checkArgument(
                valueLength >= rowOffset,
                "valueLength must be at least row offset %s, but was %s.",
                rowOffset,
                valueLength);
        return valueLength - rowOffset;
    }

    /** Reads the schema id from a raw value. */
    public short readSchemaId(MemorySegment value) {
        return readSchemaId(value, 0);
    }

    /** Reads the schema id from a raw value embedded at the given offset. */
    public short readSchemaId(MemorySegment value, int valueOffset) {
        return value.getShort(valueOffset + schemaIdOffset());
    }

    /** Writes the schema id to a raw value. */
    public void writeSchemaId(byte[] value, short schemaId) {
        writeSchemaId(value, 0, schemaId);
    }

    /** Writes the schema id to a raw value embedded at the given offset. */
    public void writeSchemaId(byte[] value, int valueOffset, short schemaId) {
        UnsafeUtils.putShort(value, valueOffset + schemaIdOffset(), schemaId);
    }

    /** Reads the internal value timestamp from a raw value. */
    public long readValueTimestamp(MemorySegment value) {
        return readValueTimestamp(value, 0);
    }

    /** Reads the internal value timestamp from a raw value embedded at the given offset. */
    public long readValueTimestamp(MemorySegment value, int valueOffset) {
        return value.getLongBigEndian(valueOffset + valueTimestampOffset());
    }

    /** Writes the internal value timestamp to a raw value. */
    public void writeValueTimestamp(byte[] value, long valueTimestampMs) {
        writeValueTimestamp(value, 0, valueTimestampMs);
    }

    /** Writes the internal value timestamp to a raw value embedded at the given offset. */
    public void writeValueTimestamp(byte[] value, int valueOffset, long valueTimestampMs) {
        MemorySegment.wrap(value)
                .putLongBigEndian(valueOffset + valueTimestampOffset(), valueTimestampMs);
    }
}
