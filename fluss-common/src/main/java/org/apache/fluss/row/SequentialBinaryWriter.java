/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.serializer.ArraySerializer;
import org.apache.fluss.row.serializer.RowSerializer;

/**
 * Writer for binary format in a sequential way. The write column position must be advanced in order
 * after each writes.
 *
 * @since 0.9
 */
@PublicEvolving
public interface SequentialBinaryWriter extends BinaryWriter {

    /** Reset writer to prepare next write. */
    void reset();

    /** Set null to this field. */
    void setNullAt(int pos);

    void writeBoolean(boolean value);

    void writeByte(byte value);

    void writeBinary(byte[] bytes, int length);

    void writeBytes(byte[] value);

    void writeChar(BinaryString value, int length);

    void writeString(BinaryString value);

    void writeShort(short value);

    void writeInt(int value);

    void writeLong(long value);

    void writeFloat(float value);

    void writeDouble(double value);

    void writeDecimal(Decimal value, int precision);

    void writeTimestampNtz(TimestampNtz value, int precision);

    void writeTimestampLtz(TimestampLtz value, int precision);

    void writeArray(InternalArray value, ArraySerializer serializer);

    // TODO: Map and Row write methods will be added in Issue #1973 and #1974
    // void writeMap(InternalMap value, InternalMapSerializer serializer);

    void writeRow(InternalRow value, RowSerializer serializer);

    /** Finally, complete write to set real size to binary. */
    void complete();

    MemorySegment segment();

    int position();

    // --------------------------------------------------------------------------------------------
    // Overrides of PositionedWriter and delegates to non-positioned methods
    // --------------------------------------------------------------------------------------------

    @Override
    default void writeBoolean(int pos, boolean value) {
        writeBoolean(value);
    }

    @Override
    default void writeByte(int pos, byte value) {
        writeByte(value);
    }

    @Override
    default void writeBytes(int pos, byte[] value) {
        writeBytes(value);
    }

    @Override
    default void writeChar(int pos, BinaryString value, int length) {
        writeChar(value, length);
    }

    @Override
    default void writeString(int pos, BinaryString value) {
        writeString(value);
    }

    @Override
    default void writeShort(int pos, short value) {
        writeShort(value);
    }

    @Override
    default void writeInt(int pos, int value) {
        writeInt(value);
    }

    @Override
    default void writeLong(int pos, long value) {
        writeLong(value);
    }

    @Override
    default void writeFloat(int pos, float value) {
        writeFloat(value);
    }

    @Override
    default void writeDouble(int pos, double value) {
        writeDouble(value);
    }

    @Override
    default void writeBinary(int pos, byte[] bytes, int length) {
        writeBinary(bytes, length);
    }

    @Override
    default void writeDecimal(int pos, Decimal value, int precision) {
        writeDecimal(value, precision);
    }

    @Override
    default void writeTimestampNtz(int pos, TimestampNtz value, int precision) {
        writeTimestampNtz(value, precision);
    }

    @Override
    default void writeTimestampLtz(int pos, TimestampLtz value, int precision) {
        writeTimestampLtz(value, precision);
    }

    @Override
    default void writeArray(int pos, InternalArray value, ArraySerializer serializer) {
        writeArray(value, serializer);
    }

    @Override
    default void writeRow(int pos, InternalRow value, RowSerializer serializer) {
        writeRow(value, serializer);
    }
}
