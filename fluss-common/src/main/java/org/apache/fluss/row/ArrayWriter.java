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

import org.apache.fluss.row.serializer.InternalArraySerializer;
import org.apache.fluss.row.serializer.Serializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.TimestampType;

import java.io.Serializable;

import static org.apache.fluss.types.DataTypeChecks.getLength;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;

/**
 * Writer to write a composite data format, like row, array. 1. Invoke {@link #reset()}. 2. Write
 * each field by writeXX or setNullAt. (Same field can not be written repeatedly.) 3. Invoke {@link
 * #complete()}.
 */
public interface ArrayWriter {

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

    void writeArray(int pos, InternalArray value, InternalArraySerializer serializer);

    // TODO: Map and Row write methods will be added in Issue #1973 and #1974
    // void writeMap(int pos, InternalMap value, InternalMapSerializer serializer);
    // void writeRow(int pos, InternalRow value, InternalRowSerializer serializer);

    /** Finally, complete write to set real size to binary. */
    void complete();

    // --------------------------------------------------------------------------------------------

    /**
     * @deprecated Use {@code #createValueSetter(DataType)} for avoiding logical types during
     *     runtime.
     */
    @Deprecated
    static void write(
            ArrayWriter writer, int pos, Object o, DataType type, Serializer<?> serializer) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                writer.writeBoolean(pos, (boolean) o);
                break;
            case CHAR:
                writer.writeChar(pos, (BinaryString) o, getLength(type));
                break;
            case TINYINT:
                writer.writeByte(pos, (byte) o);
                break;
            case SMALLINT:
                writer.writeShort(pos, (short) o);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                writer.writeInt(pos, (int) o);
                break;
            case BIGINT:
                writer.writeLong(pos, (long) o);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                writer.writeTimestampNtz(pos, (TimestampNtz) o, timestampType.getPrecision());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
                writer.writeTimestampLtz(pos, (TimestampLtz) o, lzTs.getPrecision());
                break;
            case FLOAT:
                writer.writeFloat(pos, (float) o);
                break;
            case DOUBLE:
                writer.writeDouble(pos, (double) o);
                break;

            case STRING:
                writer.writeString(pos, (BinaryString) o);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                writer.writeDecimal(pos, (Decimal) o, decimalType.getPrecision());
                break;
            case BINARY:
                int length = getLength(type);
                writer.writeBinary(pos, (byte[]) o, length);
                break;
            case ARRAY:
                writer.writeArray(pos, (InternalArray) o, (InternalArraySerializer) serializer);
                break;

            case MAP:
                // TODO: Map type support will be added in Issue #1973
                throw new UnsupportedOperationException(
                        "Map type is not supported yet. Will be added in Issue #1973.");
            case ROW:
                // TODO: Row type support will be added in Issue #1974
                throw new UnsupportedOperationException(
                        "Row type is not supported yet. Will be added in Issue #1974.");
            default:
                throw new UnsupportedOperationException("Not support type: " + type);
        }
    }

    /**
     * Creates an accessor for setting the elements of a binary writer during runtime.
     *
     * @param elementType the element type
     */
    static ValueSetter createValueSetter(DataType elementType) {
        return createValueSetter(elementType, null);
    }

    static ValueSetter createValueSetter(DataType elementType, Serializer<?> serializer) {
        // ordered by type root definition
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
                final InternalArraySerializer arraySerializer =
                        (InternalArraySerializer) serializer;
                return (writer, pos, value) ->
                        writer.writeArray(pos, (InternalArray) value, arraySerializer);

            case MAP:
                // TODO: Map type support will be added in Issue #1973
                throw new UnsupportedOperationException(
                        "Map type is not supported yet. Will be added in Issue #1973.");
            case ROW:
                // TODO: Row type support will be added in Issue #1974
                throw new UnsupportedOperationException(
                        "Row type is not supported yet. Will be added in Issue #1974.");
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                elementType.getTypeRoot().toString(), BinaryArray.class.getName());
                throw new IllegalArgumentException(msg);
        }
    }

    /** Accessor for setting the elements of a binary writer during runtime. */
    interface ValueSetter extends Serializable {
        void setValue(ArrayWriter writer, int pos, Object value);
    }
}
