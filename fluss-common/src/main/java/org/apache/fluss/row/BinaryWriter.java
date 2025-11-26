/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.BinaryRow.BinaryRowFormat;
import org.apache.fluss.row.serializer.ArraySerializer;
import org.apache.fluss.row.serializer.RowSerializer;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.fluss.types.DataTypeChecks.getLength;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;

/**
 * Writer to write a composite data format, like row, array. 1. Invoke {@link #reset()}. 2. Write
 * each field by writeXX or setNullAt. (Same field can not be written repeatedly.) 3. Invoke {@link
 * #complete()}.
 *
 * @since 0.9
 */
@PublicEvolving
public interface BinaryWriter {

    /** Reset writer to prepare next write. */
    void reset();

    /** Set null to this field. */
    void setNullAt(int pos);

    void writeBoolean(int pos, boolean value);

    void writeByte(int pos, byte value);

    void writeBytes(int pos, byte[] value);

    void writeChar(int pos, BinaryString value, int length);

    void writeString(int pos, BinaryString value);

    void writeShort(int pos, short value);

    void writeInt(int pos, int value);

    void writeLong(int pos, long value);

    void writeFloat(int pos, float value);

    void writeDouble(int pos, double value);

    void writeBinary(int pos, byte[] bytes, int length);

    void writeDecimal(int pos, Decimal value, int precision);

    void writeTimestampNtz(int pos, TimestampNtz value, int precision);

    void writeTimestampLtz(int pos, TimestampLtz value, int precision);

    void writeArray(int pos, InternalArray value, ArraySerializer serializer);

    void writeRow(int pos, InternalRow value, RowSerializer serializer);

    /** Finally, complete write to set real size to binary. */
    void complete();

    // ============================================================================================
    // --------------------------------------------------------------------------------------------

    /**
     * Creates an accessor for setting the elements of a binary writer during runtime.
     *
     * @param elementType the element type
     * @param rowFormat the binary row format, it is required when the element type has nested row
     *     type, otherwise, {@link IllegalArgumentException} will be thrown.
     */
    static BinaryWriter.ValueWriter createValueWriter(
            DataType elementType, BinaryRowFormat rowFormat) {
        BinaryWriter.ValueWriter valueWriter = createNotNullValueWriter(elementType, rowFormat);
        if (!elementType.isNullable()) {
            return valueWriter;
        }
        // wrap null setter
        return (writer, pos, value) -> {
            if (value == null) {
                writer.setNullAt(pos);
            } else {
                valueWriter.writeValue(writer, pos, value);
            }
        };
    }

    /**
     * Creates an accessor for setting the elements of a binary writer during runtime.
     *
     * @param elementType the element type
     */
    static BinaryWriter.ValueWriter createNotNullValueWriter(
            DataType elementType, @Nullable BinaryRowFormat rowFormat) {
        switch (elementType.getTypeRoot()) {
            case CHAR:
                int charLength = getLength(elementType);
                return (writer, pos, value) ->
                        writer.writeChar(pos, (BinaryString) value, charLength);
            case STRING:
                return (writer, pos, value) -> writer.writeString(pos, (BinaryString) value);
            case BOOLEAN:
                return (writer, pos, value) -> writer.writeBoolean(pos, (boolean) value);
            case BINARY:
                final int binaryLength = getLength(elementType);
                return (writer, pos, value) ->
                        writer.writeBinary(pos, (byte[]) value, binaryLength);
            case BYTES:
                return (writer, pos, value) -> writer.writeBytes(pos, (byte[]) value);
            case DECIMAL:
                final int decimalPrecision = getPrecision(elementType);
                return (writer, pos, value) ->
                        writer.writeDecimal(pos, (Decimal) value, decimalPrecision);
            case TINYINT:
                return (writer, pos, value) -> writer.writeByte(pos, (byte) value);
            case SMALLINT:
                return (writer, pos, value) -> writer.writeShort(pos, (short) value);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return (writer, pos, value) -> writer.writeInt(pos, (int) value);
            case BIGINT:
                return (writer, pos, value) -> writer.writeLong(pos, (long) value);
            case FLOAT:
                return (writer, pos, value) -> writer.writeFloat(pos, (float) value);
            case DOUBLE:
                return (writer, pos, value) -> writer.writeDouble(pos, (double) value);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(elementType);
                return (writer, pos, value) ->
                        writer.writeTimestampNtz(pos, (TimestampNtz) value, timestampNtzPrecision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(elementType);
                return (writer, pos, value) ->
                        writer.writeTimestampLtz(pos, (TimestampLtz) value, timestampLtzPrecision);
            case ARRAY:
                final ArraySerializer arraySerializer =
                        new ArraySerializer(((ArrayType) elementType).getElementType(), rowFormat);
                return (writer, pos, value) ->
                        writer.writeArray(pos, (InternalArray) value, arraySerializer);

            case MAP:
                // TODO: Map type support will be added in Issue #1973
                throw new UnsupportedOperationException(
                        "Map type is not supported yet. Will be added in Issue #1973.");
            case ROW:
                if (rowFormat == null) {
                    throw new IllegalArgumentException(
                            "Binary row format is required to write row.");
                }
                final RowType rowType = (RowType) elementType;
                final RowSerializer rowSerializer =
                        new RowSerializer(
                                rowType.getFieldTypes().toArray(new DataType[0]), rowFormat);
                return (writer, pos, value) ->
                        writer.writeRow(pos, (InternalRow) value, rowSerializer);
            default:
                String msg =
                        String.format(
                                "Type %s not supported yet", elementType.getTypeRoot().toString());
                throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Accessor for writing the fields/elements of a binary writer during runtime, the
     * fields/elements are must be written in the order.
     */
    interface ValueWriter extends Serializable {
        void writeValue(BinaryWriter writer, int pos, Object value);
    }
}
