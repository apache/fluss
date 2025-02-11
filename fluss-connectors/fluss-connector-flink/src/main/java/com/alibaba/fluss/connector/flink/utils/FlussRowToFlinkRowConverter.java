/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.utils;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import java.io.Serializable;

/**
 * A converter to convert Fluss's {@link InternalRow} to Flink's {@link RowData}.
 *
 * <p>Note: fluss-datalake-tiering also contains the same class, we need to keep them in sync if we
 * modify this class.
 */
public class FlussRowToFlinkRowConverter {

    private final FlussDeserializationConverter[] toFlinkFieldConverters;
    private final InternalRow.FieldGetter[] flussFieldGetters;

    public FlussRowToFlinkRowConverter(RowType rowType) {
        this.toFlinkFieldConverters = new FlussDeserializationConverter[rowType.getFieldCount()];
        this.flussFieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toFlinkFieldConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
            flussFieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }
    }

    public RowData toFlinkRowData(ScanRecord scanRecord) {
        return toFlinkRowData(scanRecord.getRow(), toFlinkRowKind(scanRecord.getRowKind()));
    }

    public RowData toFlinkRowData(InternalRow flussRow) {
        return toFlinkRowData(flussRow, RowKind.INSERT);
    }

    private RowData toFlinkRowData(InternalRow flussRow, RowKind rowKind) {
        GenericRowData genericRowData = new GenericRowData(toFlinkFieldConverters.length);
        genericRowData.setRowKind(rowKind);
        for (int i = 0; i < toFlinkFieldConverters.length; i++) {
            Object flussField = flussFieldGetters[i].getFieldOrNull(flussRow);
            genericRowData.setField(i, toFlinkFieldConverters[i].deserialize(flussField));
        }
        return genericRowData;
    }

    private RowKind toFlinkRowKind(com.alibaba.fluss.record.RowKind rowKind) {
        switch (rowKind) {
            case APPEND_ONLY:
            case INSERT:
                return RowKind.INSERT;
            case UPDATE_BEFORE:
                return RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return RowKind.UPDATE_AFTER;
            case DELETE:
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException("Unsupported row kind: " + rowKind);
        }
    }

    /**
     * Create a nullable runtime {@link FlussDeserializationConverter} from given {@link DataType}.
     */
    protected FlussDeserializationConverter createNullableInternalConverter(
            DataType flussDataType) {
        return wrapIntoNullableInternalConverter(createInternalConverter(flussDataType));
    }

    protected FlussDeserializationConverter wrapIntoNullableInternalConverter(
            FlussDeserializationConverter flussDeserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return flussDeserializationConverter.deserialize(val);
            }
        };
    }

    /**
     * Runtime converter to convert field in Fluss's {@link InternalRow} to Flink's {@link RowData}
     * type object.
     */
    @FunctionalInterface
    public interface FlussDeserializationConverter extends Serializable {

        /**
         * Convert a Fluss field object of {@link InternalRow} to the Flink's internal data
         * structure object.
         *
         * @param flussField A single field of a {@link InternalRow}
         */
        Object deserialize(Object flussField);
    }

    // TODO: use flink row type
    private FlussDeserializationConverter createInternalConverter(DataType flussDataType) {
        switch (flussDataType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return (flussField) -> flussField;
            case CHAR:
            case STRING:
                return (flussField) -> StringData.fromBytes(((BinaryString) flussField).toBytes());
            case BYTES:
            case BINARY:
                return (flussField) -> flussField;
            case DECIMAL:
                return (flussField) -> {
                    Decimal decimal = (Decimal) flussField;
                    return DecimalData.fromBigDecimal(
                            decimal.toBigDecimal(), decimal.precision(), decimal.scale());
                };
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return (flussField) -> flussField;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (flussField) -> {
                    TimestampNtz timestampNtz = (TimestampNtz) flussField;
                    return TimestampData.fromLocalDateTime(timestampNtz.toLocalDateTime());
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (flussField) -> {
                    TimestampLtz timestampLtz = (TimestampLtz) flussField;
                    return TimestampData.fromEpochMillis(
                            timestampLtz.getEpochMillisecond(),
                            timestampLtz.getNanoOfMillisecond());
                };
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + flussDataType);
        }
    }
}
