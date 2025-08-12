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

package com.alibaba.fluss.lake.iceberg.tiering;

import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.lake.iceberg.utils.IcebergConversions.toFlinkLogicalType;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * Converts Fluss {@link LogRecord} to Flink {@link RowData} compatible with Iceberg TaskWriter.
 *
 * <p>This class handles runtime record conversion, converting actual data values from Fluss format
 * to Flink RowData format that Iceberg's TaskWriter can consume.
 */
public class FlussRecordAsIcebergRow implements RowData {

    private static final int LAKE_ICEBERG_SYSTEM_COLUMNS = 3;
    private static final String BUCKET_COLUMN_NAME = "__bucket";
    private static final String OFFSET_COLUMN_NAME = "__offset";
    private static final String TIMESTAMP_COLUMN_NAME = "__timestamp";

    private final Schema icebergSchema;
    private final RowType flinkRowType;
    private final int bucket;
    private LogRecord logRecord;
    private int originRowFieldCount;
    private com.alibaba.fluss.row.InternalRow internalRow;
    private GenericRowData rowData;

    public FlussRecordAsIcebergRow(int bucket, Schema icebergSchema) {
        this.bucket = bucket;
        this.icebergSchema = icebergSchema;
        this.flinkRowType = createFlinkRowType(icebergSchema);
    }

    public void setFlussRecord(LogRecord logRecord) {
        this.logRecord = logRecord;
        this.internalRow = logRecord.getRow();
        this.originRowFieldCount = internalRow.getFieldCount();

        checkState(
                originRowFieldCount == icebergSchema.columns().size() - LAKE_ICEBERG_SYSTEM_COLUMNS,
                "The Iceberg table fields count must equal LogRecord's fields count plus system columns.");

        // Convert to Flink RowData
        this.rowData = convertToRowData();
    }

    public RowType getFlinkRowType() {
        return flinkRowType;
    }

    private GenericRowData convertToRowData() {
        int totalFields = originRowFieldCount + LAKE_ICEBERG_SYSTEM_COLUMNS;
        GenericRowData rowData = new GenericRowData(totalFields);

        for (int i = 0; i < originRowFieldCount; i++) {
            Object value =
                    convertFlussValueToFlink(internalRow, i, icebergSchema.columns().get(i).type());
            rowData.setField(i, value);
        }

        rowData.setField(originRowFieldCount, bucket);
        rowData.setField(originRowFieldCount + 1, logRecord.logOffset());
        rowData.setField(
                originRowFieldCount + 2, TimestampData.fromEpochMillis(logRecord.timestamp()));

        return rowData;
    }

    private Object convertFlussValueToFlink(
            com.alibaba.fluss.row.InternalRow row, int pos, Type icebergType) {
        if (row.isNullAt(pos)) {
            return null;
        }

        switch (icebergType.typeId()) {
            case BOOLEAN:
                return row.getBoolean(pos);
            case INTEGER:
                return row.getInt(pos);
            case LONG:
                return row.getLong(pos);
            case FLOAT:
                return row.getFloat(pos);
            case DOUBLE:
                return row.getDouble(pos);
            case STRING:
                return StringData.fromString(row.getString(pos).toString());
            case BINARY:
                int binaryLength =
                        icebergType.typeId() == Type.TypeID.FIXED
                                ? ((Types.FixedType) icebergType).length()
                                : BinaryType.MAX_LENGTH; // Default max for variable binary
                return row.getBinary(pos, binaryLength);
            case TIMESTAMP:
                // with and without Timezone
                Types.TimestampType timestampType = (Types.TimestampType) icebergType;
                if (timestampType.shouldAdjustToUTC()) {
                    TimestampLtz tsLtz = row.getTimestampLtz(pos, 6);
                    return TimestampData.fromEpochMillis(
                            tsLtz.getEpochMillisecond(), tsLtz.getNanoOfMillisecond());
                } else {
                    TimestampNtz tsNtz = row.getTimestampNtz(pos, 6);
                    return TimestampData.fromEpochMillis(
                            tsNtz.getMillisecond(), tsNtz.getNanoOfMillisecond());
                }
            case DECIMAL:
                Types.DecimalType decimalType = (Types.DecimalType) icebergType;
                return row.getDecimal(pos, decimalType.precision(), decimalType.scale());
            default:
                throw new UnsupportedOperationException("Unsupported Iceberg type: " + icebergType);
        }
    }

    private static RowType createFlinkRowType(Schema icebergSchema) {
        List<RowType.RowField> fields = new ArrayList<>();

        for (Types.NestedField column : icebergSchema.columns()) {
            if (!isSystemColumn(column.name())) {
                LogicalType flinkType = toFlinkLogicalType(column.type());
                fields.add(new RowType.RowField(column.name(), flinkType));
            }
        }

        fields.add(new RowType.RowField(BUCKET_COLUMN_NAME, new IntType()));
        fields.add(new RowType.RowField(OFFSET_COLUMN_NAME, new BigIntType()));
        fields.add(new RowType.RowField(TIMESTAMP_COLUMN_NAME, new TimestampType(3)));

        return new RowType(fields);
    }

    private static boolean isSystemColumn(String columnName) {
        return BUCKET_COLUMN_NAME.equals(columnName)
                || OFFSET_COLUMN_NAME.equals(columnName)
                || TIMESTAMP_COLUMN_NAME.equals(columnName);
    }

    @Override
    public int getArity() {
        return rowData.getArity();
    }

    @Override
    public RowKind getRowKind() {
        switch (logRecord.getChangeType()) {
            case INSERT:
            case APPEND_ONLY:
                return RowKind.INSERT;
            case UPDATE_BEFORE:
                return RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return RowKind.UPDATE_AFTER;
            case DELETE:
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException(
                        "Unsupported change type: " + logRecord.getChangeType());
        }
    }

    @Override
    public void setRowKind(RowKind kind) {
        rowData.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        return rowData.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return rowData.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return rowData.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return rowData.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return rowData.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return rowData.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return rowData.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return rowData.getDouble(pos);
    }

    @Override
    public StringData getString(int pos) {
        return rowData.getString(pos);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return rowData.getDecimal(pos, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return rowData.getTimestamp(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return rowData.getBinary(pos);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int i) {
        throw new UnsupportedOperationException(
                "getRawValue is not support for Fluss record currently.");
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        throw new UnsupportedOperationException(
                "getRow is not support for Fluss record currently.");
    }

    @Override
    public ArrayData getArray(int pos) {
        throw new UnsupportedOperationException(
                "getArray is not support for Fluss record currently.");
    }

    @Override
    public MapData getMap(int pos) {
        throw new UnsupportedOperationException(
                "getMap is not support for Fluss record currently.");
    }
}
