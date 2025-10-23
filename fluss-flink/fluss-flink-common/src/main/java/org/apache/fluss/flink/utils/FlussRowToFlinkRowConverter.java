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

package org.apache.fluss.flink.utils;

import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import java.io.Serializable;

import static org.apache.fluss.flink.utils.FlinkConversions.toFlinkRowKind;

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

    public RowData toFlinkRowData(LogRecord logRecord) {
        return toFlinkRowData(logRecord.getRow(), toFlinkRowKind(logRecord.getChangeType()));
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
