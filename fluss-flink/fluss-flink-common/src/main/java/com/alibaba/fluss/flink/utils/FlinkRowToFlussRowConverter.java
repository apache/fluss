/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.flink.utils;

import com.alibaba.fluss.flink.row.FlinkAsFlussRow;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.encode.IndexedRowEncoder;
import com.alibaba.fluss.row.encode.RowEncoder;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.DataType;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

/**
 * A converter to convert Flink's {@link RowData} to Fluss's {@link InternalRow}.
 *
 * @deprecated This class is deprecated and will be removed in future versions. It is recommended to
 *     use the {@link FlinkAsFlussRow} to wraps Flink's {@link RowData} as Fluss's {@link
 *     InternalRow}.
 */
@Deprecated
public class FlinkRowToFlussRowConverter implements AutoCloseable {

    private final int fieldLength;
    private final RowData.FieldGetter[] fieldGetters;
    private final FlussSerializationConverter[] toFlussFieldConverters;
    private final RowEncoder rowEncoder;

    /** Create a {@link FlinkRowToFlussRowConverter} which will convert to {@link IndexedRow}. */
    public static FlinkRowToFlussRowConverter create(RowType flinkRowType) {
        DataType[] flussFieldTypes = toFlussDataTypes(flinkRowType);
        RowEncoder rowEncoder = new IndexedRowEncoder(flussFieldTypes);
        return new FlinkRowToFlussRowConverter(flinkRowType, rowEncoder);
    }

    /** Create a {@link FlinkRowToFlussRowConverter} according to the given {@link KvFormat}. */
    public static FlinkRowToFlussRowConverter create(RowType flinkRowType, KvFormat kvFormat) {
        DataType[] flussFieldTypes = toFlussDataTypes(flinkRowType);
        RowEncoder rowEncoder = RowEncoder.create(kvFormat, flussFieldTypes);
        return new FlinkRowToFlussRowConverter(flinkRowType, rowEncoder);
    }

    private static DataType[] toFlussDataTypes(RowType flinkRowType) {
        return FlinkConversions.toFlussRowType(flinkRowType).getChildren().toArray(new DataType[0]);
    }

    private FlinkRowToFlussRowConverter(RowType flinkRowType, RowEncoder rowEncoder) {
        this.fieldLength = flinkRowType.getFieldCount();

        // create field getter to get field from flink row.
        this.fieldGetters = new RowData.FieldGetter[fieldLength];
        for (int i = 0; i < fieldLength; i++) {
            this.fieldGetters[i] = RowData.createFieldGetter(flinkRowType.getTypeAt(i), i);
        }

        // create field converter to convert field from flink to fluss.
        this.toFlussFieldConverters = new FlussSerializationConverter[fieldLength];
        for (int i = 0; i < fieldLength; i++) {
            toFlussFieldConverters[i] = createNullableInternalConverter(flinkRowType.getTypeAt(i));
        }
        this.rowEncoder = rowEncoder;
    }

    public InternalRow toInternalRow(RowData rowData) {
        rowEncoder.startNewRow();
        for (int i = 0; i < fieldLength; i++) {
            rowEncoder.encodeField(
                    i,
                    toFlussFieldConverters[i].serialize(fieldGetters[i].getFieldOrNull(rowData)));
        }
        return rowEncoder.finishRow();
    }

    private FlussSerializationConverter createNullableInternalConverter(LogicalType flinkField) {
        return wrapIntoNullableInternalConverter(createInternalConverter(flinkField));
    }

    private FlussSerializationConverter wrapIntoNullableInternalConverter(
            FlussSerializationConverter flussSerializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return flussSerializationConverter.serialize(val);
            }
        };
    }

    @Override
    public void close() throws Exception {
        if (rowEncoder != null) {
            rowEncoder.close();
        }
    }

    /**
     * Runtime converter to convert field in Flink's {@link RowData} to Fluss's {@link InternalRow}
     * type object.
     */
    @FunctionalInterface
    public interface FlussSerializationConverter extends Serializable {
        /**
         * Convert a Flink field object of {@link RowData} to the Fluss's internal data structure
         * object.
         *
         * @param flinkField A single field of a {@link RowData}
         */
        Object serialize(Object flinkField);
    }

    private FlussSerializationConverter createInternalConverter(LogicalType flinkDataType) {
        switch (flinkDataType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case VARBINARY:
                return (flinkField) -> flinkField;
            case CHAR:
            case VARCHAR:
                return (flinkField) -> {
                    StringData stringData = (StringData) flinkField;
                    return BinaryString.fromBytes(stringData.toBytes());
                };
            case DECIMAL:
                return (flinkField) -> {
                    DecimalData decimalData = (DecimalData) flinkField;
                    return Decimal.fromBigDecimal(
                            decimalData.toBigDecimal(),
                            decimalData.precision(),
                            decimalData.scale());
                };
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return (flussField) -> flussField;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (flinkField) -> {
                    TimestampData timestampData = (TimestampData) flinkField;
                    return TimestampNtz.fromLocalDateTime(timestampData.toLocalDateTime());
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (flinkField) -> {
                    TimestampData timestampData = (TimestampData) flinkField;
                    return TimestampLtz.fromEpochMillis(
                            timestampData.getMillisecond(), timestampData.getNanoOfMillisecond());
                };
            default:
                throw new UnsupportedOperationException(
                        "Fluss Unsupported data type: " + flinkDataType);
        }
    }
}
