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

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.compacted.CompactedRowSerializer;
import org.apache.fluss.row.indexed.IndexedRowSerializer;
import org.apache.fluss.row.serializer.InternalArraySerializer;
import org.apache.fluss.row.serializer.InternalMapSerializer;
import org.apache.fluss.row.serializer.InternalRowSerializer;
import org.apache.fluss.row.serializer.InternalSerializers;
import org.apache.fluss.row.serializer.Serializer;
import org.apache.fluss.types.DataType;

import java.io.Closeable;
import java.io.Serializable;

import static org.apache.fluss.types.DataTypeChecks.getLength;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;

/**
 * Writer to write a composite data format, like row, array. 1. Invoke {@link #reset()}. 2. Write
 * each field by writeXX or setNullAt. (Same field can not be written repeatedly.) 3. Invoke {@link
 * #complete()}.
 */
public interface BinaryWriter extends Closeable {

    /** Reset writer to prepare next write. */
    void reset();

    /** Set null to this field. */
    void setNullAt(int pos);

    void writeBoolean(boolean value);

    void writeByte(byte value);

    void writeBytes(byte[] value);

    void writeChar(BinaryString value, int length);

    void writeString(BinaryString value);

    void writeShort(short value);

    void writeInt(int value);

    void writeLong(long value);

    void writeFloat(float value);

    void writeDouble(double value);

    void writeBinary(byte[] bytes, int length);

    void writeDecimal(Decimal value, int precision);

    void writeTimestampNtz(TimestampNtz value, int precision);

    void writeTimestampLtz(TimestampLtz value, int precision);

    void writeArray(InternalArray value, InternalArraySerializer serializer);

    void writeMap(InternalMap value, InternalMapSerializer serializer);

    void writeRow(InternalRow value, InternalRowSerializer serializer);

    /** Finally, complete write to set real size to binary. */
    void complete();

    MemorySegment segment();

    int position();

    // --------------------------------------------------------------------------------------------

    /**
     * Creates an accessor for setting the elements of a binary writer during runtime.
     *
     * @param elementType the element type
     */
    static ValueSetter createValueSetter(DataType elementType) {
        return createValueSetter(elementType, null, KvFormat.INDEXED);
    }

    static ValueSetter createValueSetter(
            DataType elementType, Serializer<?> serializer, KvFormat kvFormat) {
        // ordered by type root definition
        switch (elementType.getTypeRoot()) {
            case CHAR:
                int charLength = getLength(elementType);
                return (writer, pos, value) -> writer.writeChar((BinaryString) value, charLength);
            case STRING:
                return (writer, pos, value) -> writer.writeString((BinaryString) value);
            case BOOLEAN:
                return (writer, pos, value) -> writer.writeBoolean((boolean) value);
            case BINARY:
                final int binaryLength = getLength(elementType);
                return (writer, pos, value) -> writer.writeBinary((byte[]) value, binaryLength);
            case DECIMAL:
                final int decimalPrecision = getPrecision(elementType);
                return (writer, pos, value) ->
                        writer.writeDecimal((Decimal) value, decimalPrecision);
            case TINYINT:
                return (writer, pos, value) -> writer.writeByte((byte) value);
            case SMALLINT:
                return (writer, pos, value) -> writer.writeShort((short) value);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return (writer, pos, value) -> writer.writeInt((int) value);
            case BIGINT:
                return (writer, pos, value) -> writer.writeLong((long) value);
            case FLOAT:
                return (writer, pos, value) -> writer.writeFloat((float) value);
            case DOUBLE:
                return (writer, pos, value) -> writer.writeDouble((double) value);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(elementType);
                return (writer, pos, value) ->
                        writer.writeTimestampNtz((TimestampNtz) value, timestampNtzPrecision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(elementType);
                return (writer, pos, value) ->
                        writer.writeTimestampLtz((TimestampLtz) value, timestampLtzPrecision);
            case ARRAY:
                final Serializer<?> arraySerializer =
                        serializer == null ? InternalSerializers.create(elementType) : serializer;
                return (writer, pos, value) ->
                        writer.writeArray(
                                (InternalArray) value, (InternalArraySerializer) arraySerializer);

            case MAP:
                final Serializer<?> mapSerializer =
                        serializer == null ? InternalSerializers.create(elementType) : serializer;
                return (writer, pos, value) ->
                        writer.writeMap((InternalMap) value, (InternalMapSerializer) mapSerializer);
            case ROW:
                final Serializer<?> rowSerializer;
                if (kvFormat == KvFormat.INDEXED) {
                    rowSerializer =
                            serializer == null
                                    ? IndexedRowSerializer.create(elementType)
                                    : serializer;
                } else {
                    rowSerializer =
                            serializer == null
                                    ? CompactedRowSerializer.create(elementType)
                                    : serializer;
                }
                return (writer, pos, value) ->
                        writer.writeRow((InternalRow) value, (InternalRowSerializer) rowSerializer);
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                elementType.getTypeRoot().toString(), BinaryRow.class.getName());
                throw new IllegalArgumentException(msg);
        }
    }

    /** Accessor for setting the elements of a binary writer during runtime. */
    interface ValueSetter extends Serializable {
        void setValue(BinaryWriter writer, int pos, Object value);
    }
}
