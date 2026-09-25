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

package org.apache.fluss.trino;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.DataGetters;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimestampType;

import io.airlift.slice.Slice;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.Chars;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;

import java.util.List;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Writes supported Fluss values into driver-owned Trino blocks. */
final class FlussRowDecoder {
    private final List<FlussColumnHandle> columns;
    private final List<Type> types;
    private final List<DataType> dataTypes;

    FlussRowDecoder(Schema schema, List<FlussColumnHandle> columns) {
        checkNotNull(schema, "schema is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        ImmutableList.Builder<DataType> dataTypes = ImmutableList.builder();
        for (FlussColumnHandle column : columns) {
            int ordinal = column.getOrdinalPosition();
            checkArgument(ordinal < schema.getColumns().size(), "Column ordinal exceeds schema");
            Schema.Column field = schema.getColumns().get(ordinal);
            checkArgument(
                    field.getName().equals(column.getName()),
                    "Column does not match current table schema");
            DataType dataType = field.getDataType();
            dataTypes.add(dataType);
            types.add(FlussTypeConverter.toTrinoType(dataType));
        }
        this.types = types.build();
        this.dataTypes = dataTypes.build();
    }

    List<Type> getTypes() {
        return types;
    }

    void append(InternalRow row, PageBuilder builder) {
        for (int channel = 0; channel < columns.size(); channel++) {
            writeValue(
                    row,
                    columns.get(channel).getOrdinalPosition(),
                    dataTypes.get(channel),
                    types.get(channel),
                    builder.getBlockBuilder(channel));
        }
        builder.declarePosition();
    }

    private static void writeValue(
            DataGetters values, int position, DataType dataType, Type type, BlockBuilder block) {
        if (values.isNullAt(position)) {
            block.appendNull();
            return;
        }
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                type.writeBoolean(block, values.getBoolean(position));
                break;
            case TINYINT:
                type.writeLong(block, values.getByte(position));
                break;
            case SMALLINT:
                type.writeLong(block, values.getShort(position));
                break;
            case INTEGER:
            case DATE:
                type.writeLong(block, values.getInt(position));
                break;
            case BIGINT:
                type.writeLong(block, values.getLong(position));
                break;
            case FLOAT:
                type.writeLong(block, Float.floatToRawIntBits(values.getFloat(position)));
                break;
            case DOUBLE:
                type.writeDouble(block, values.getDouble(position));
                break;
            case CHAR:
                // Trino CHAR stores unpadded UTF-8; oversized Fluss CHAR maps to VARCHAR.
                Slice chars =
                        wrappedBuffer(
                                values.getChar(position, ((CharType) dataType).getLength())
                                        .toBytes());
                type.writeSlice(
                        block,
                        type instanceof io.trino.spi.type.CharType
                                ? Chars.trimTrailingSpaces(chars)
                                : chars);
                break;
            case STRING:
                type.writeSlice(block, wrappedBuffer(values.getString(position).toBytes()));
                break;
            case BINARY:
                type.writeSlice(
                        block,
                        wrappedBuffer(
                                values.getBinary(position, ((BinaryType) dataType).getLength())));
                break;
            case BYTES:
                type.writeSlice(block, wrappedBuffer(values.getBytes(position)));
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                Decimal decimal =
                        values.getDecimal(
                                position, decimalType.getPrecision(), decimalType.getScale());
                if (((io.trino.spi.type.DecimalType) type).isShort()) {
                    type.writeLong(block, decimal.toUnscaledLong());
                } else {
                    type.writeObject(block, Int128.valueOf(decimal.toBigDecimal().unscaledValue()));
                }
                break;
            case TIME_WITHOUT_TIME_ZONE:
                // Fluss InternalRow stores time as milliseconds of the day.
                type.writeLong(block, values.getInt(position) * 1_000_000_000L);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampNtz timestamp =
                        values.getTimestampNtz(position, ((TimestampType) dataType).getPrecision());
                long epochMicros =
                        Math.addExact(
                                Math.multiplyExact(timestamp.getMillisecond(), 1000),
                                timestamp.getNanoOfMillisecond() / 1000);
                if (((io.trino.spi.type.TimestampType) type).isShort()) {
                    type.writeLong(block, epochMicros);
                } else {
                    type.writeObject(
                            block,
                            new LongTimestamp(
                                    epochMicros, (timestamp.getNanoOfMillisecond() % 1000) * 1000));
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampLtz instant =
                        values.getTimestampLtz(
                                position, ((LocalZonedTimestampType) dataType).getPrecision());
                if (((io.trino.spi.type.TimestampWithTimeZoneType) type).isShort()) {
                    type.writeLong(
                            block, packDateTimeWithZone(instant.getEpochMillisecond(), UTC_KEY));
                } else {
                    type.writeObject(
                            block,
                            LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                                    instant.getEpochMillisecond(),
                                    instant.getNanoOfMillisecond() * 1000,
                                    UTC_KEY));
                }
                break;
            case ARRAY:
                InternalArray array = values.getArray(position);
                DataType elementDataType = ((ArrayType) dataType).getElementType();
                Type elementType = ((io.trino.spi.type.ArrayType) type).getElementType();
                ((ArrayBlockBuilder) block)
                        .buildEntry(
                                elements -> {
                                    for (int i = 0; i < array.size(); i++) {
                                        writeValue(
                                                array, i, elementDataType, elementType, elements);
                                    }
                                });
                break;
            case MAP:
                InternalMap map = values.getMap(position);
                InternalArray keys = map.keyArray();
                InternalArray mapValues = map.valueArray();
                MapType mapDataType = (MapType) dataType;
                io.trino.spi.type.MapType mapType = (io.trino.spi.type.MapType) type;
                for (int i = 0; i < map.size(); i++) {
                    if (keys.isNullAt(i)) {
                        throw new TrinoException(
                                GENERIC_INTERNAL_ERROR, "Fluss value contains a null map key");
                    }
                }
                ((MapBlockBuilder) block)
                        .buildEntry(
                                (keyBlock, valueBlock) -> {
                                    for (int i = 0; i < map.size(); i++) {
                                        writeValue(
                                                keys,
                                                i,
                                                mapDataType.getKeyType(),
                                                mapType.getKeyType(),
                                                keyBlock);
                                        writeValue(
                                                mapValues,
                                                i,
                                                mapDataType.getValueType(),
                                                mapType.getValueType(),
                                                valueBlock);
                                    }
                                });
                break;
            case ROW:
                RowType rowDataType = (RowType) dataType;
                InternalRow row = values.getRow(position, rowDataType.getFieldCount());
                List<Type> fieldTypes = type.getTypeParameters();
                ((RowBlockBuilder) block)
                        .buildEntry(
                                fields -> {
                                    for (int i = 0; i < rowDataType.getFieldCount(); i++) {
                                        writeValue(
                                                row,
                                                i,
                                                rowDataType.getFields().get(i).getType(),
                                                fieldTypes.get(i),
                                                fields.get(i));
                                    }
                                });
                break;
            default:
                throw new TrinoException(
                        NOT_SUPPORTED,
                        "Unsupported Fluss read type: " + dataType.asSummaryString());
        }
    }
}
