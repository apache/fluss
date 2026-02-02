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

import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * A converter that transforms Fluss's {@link LogRecord} to Flink's {@link RowData} with nested
 * before/after row structure for the $binlog virtual table.
 */
public class BinlogRowConverter implements RecordToFlinkRowConverter {

    private final FlussRowToFlinkRowConverter baseConverter;
    private final org.apache.flink.table.types.logical.RowType producedType;

    /**
     * Buffer for the UPDATE_BEFORE (-U) record pending merge with the next UPDATE_AFTER (+U)
     * record. Null when no update is in progress.
     */
    @Nullable private LogRecord pendingUpdateBefore;

    /** Creates a new BinlogRowConverter. */
    public BinlogRowConverter(RowType rowType) {
        this.baseConverter = new FlussRowToFlinkRowConverter(rowType);
        this.producedType = buildBinlogRowType(FlinkConversions.toFlinkRowType(rowType));
    }

    /** Converts a LogRecord to a binlog RowData with nested before/after structure. */
    @Nullable
    public RowData toBinlogRowData(LogRecord record) {
        ChangeType changeType = record.getChangeType();

        switch (changeType) {
            case INSERT:
                return buildBinlogRow(
                        "insert",
                        record.logOffset(),
                        record.timestamp(),
                        null,
                        baseConverter.toFlinkRowData(record.getRow()));

            case UPDATE_BEFORE:
                // Buffer the -U record and return null.
                // FlinkRecordEmitter.processAndEmitRecord() skips null results.
                this.pendingUpdateBefore = record;
                return null;

            case UPDATE_AFTER:
                // Merge with the buffered -U record
                if (pendingUpdateBefore == null) {
                    throw new IllegalStateException(
                            "Received UPDATE_AFTER (+U) without a preceding UPDATE_BEFORE (-U) record. "
                                    + "This indicates a corrupted log sequence.");
                }
                RowData beforeRow = baseConverter.toFlinkRowData(pendingUpdateBefore.getRow());
                RowData afterRow = baseConverter.toFlinkRowData(record.getRow());
                // Use offset and timestamp from the -U record (first entry of update pair)
                long offset = pendingUpdateBefore.logOffset();
                long timestamp = pendingUpdateBefore.timestamp();
                pendingUpdateBefore = null;
                return buildBinlogRow("update", offset, timestamp, beforeRow, afterRow);

            case DELETE:
                return buildBinlogRow(
                        "delete",
                        record.logOffset(),
                        record.timestamp(),
                        baseConverter.toFlinkRowData(record.getRow()),
                        null);

            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "$binlog virtual table does not support change type: %s. "
                                        + "$binlog is only supported for primary key tables.",
                                changeType));
        }
    }

    @Override
    @Nullable
    public RowData convert(LogRecord record) {
        return toBinlogRowData(record);
    }

    @Override
    public org.apache.flink.table.types.logical.RowType getProducedType() {
        return producedType;
    }

    /**
     * Builds a binlog row with 5 fields: _change_type, _log_offset, _commit_timestamp, before,
     * after.
     */
    private RowData buildBinlogRow(
            String changeType,
            long offset,
            long timestamp,
            @Nullable RowData before,
            @Nullable RowData after) {
        GenericRowData row = new GenericRowData(5);
        row.setField(0, StringData.fromString(changeType));
        row.setField(1, offset);
        row.setField(2, TimestampData.fromEpochMillis(timestamp));
        row.setField(3, before);
        row.setField(4, after);
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    /**
     * Builds the Flink RowType for the binlog virtual table with nested before/after ROW columns.
     */
    public static org.apache.flink.table.types.logical.RowType buildBinlogRowType(
            org.apache.flink.table.types.logical.RowType originalType) {
        List<org.apache.flink.table.types.logical.RowType.RowField> fields = new ArrayList<>();

        // Add metadata columns
        fields.add(
                new org.apache.flink.table.types.logical.RowType.RowField(
                        TableDescriptor.CHANGE_TYPE_COLUMN, new VarCharType(false, 6)));
        fields.add(
                new org.apache.flink.table.types.logical.RowType.RowField(
                        TableDescriptor.LOG_OFFSET_COLUMN, new BigIntType(false)));
        fields.add(
                new org.apache.flink.table.types.logical.RowType.RowField(
                        TableDescriptor.COMMIT_TIMESTAMP_COLUMN,
                        new LocalZonedTimestampType(false, 3)));

        // Add nested before and after ROW columns (nullable at the ROW level)
        org.apache.flink.table.types.logical.RowType nullableRowType =
                new org.apache.flink.table.types.logical.RowType(true, originalType.getFields());
        fields.add(
                new org.apache.flink.table.types.logical.RowType.RowField(
                        TableDescriptor.BEFORE_COLUMN, nullableRowType));
        fields.add(
                new org.apache.flink.table.types.logical.RowType.RowField(
                        TableDescriptor.AFTER_COLUMN, nullableRowType));

        return new org.apache.flink.table.types.logical.RowType(fields);
    }
}
