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
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.List;

/**
 * A converter that transforms Fluss's {@link LogRecord} to Flink's {@link RowData} with additional
 * metadata columns for the $changelog virtual table.
 */
public class ChangelogRowConverter implements RecordToFlinkRowConverter {

    private final FlussRowToFlinkRowConverter baseConverter;
    private final org.apache.flink.table.types.logical.RowType producedType;

    /** Creates a new ChangelogRowConverter. */
    public ChangelogRowConverter(RowType rowType) {
        this.baseConverter = new FlussRowToFlinkRowConverter(rowType);
        this.producedType = buildChangelogRowType(FlinkConversions.toFlinkRowType(rowType));
    }

    /** Converts a LogRecord to a Flink RowData with metadata columns. */
    public RowData toChangelogRowData(LogRecord record) {
        RowData physicalRowData = baseConverter.toFlinkRowData(record.getRow());

        // Create metadata row with 3 fields
        GenericRowData metadataRow = new GenericRowData(3);

        // 1. _change_type
        String changeTypeStr = convertChangeTypeToString(record.getChangeType());
        metadataRow.setField(0, StringData.fromString(changeTypeStr));

        // 2. _log_offset
        metadataRow.setField(1, record.logOffset());

        // 3. _commit_timestamp (convert long to TimestampData)
        metadataRow.setField(2, TimestampData.fromEpochMillis(record.timestamp()));

        // Use JoinedRowData to efficiently combine metadata and physical rows
        JoinedRowData joinedRow = new JoinedRowData(metadataRow, physicalRowData);
        joinedRow.setRowKind(RowKind.INSERT);

        return joinedRow;
    }

    @Override
    public RowData convert(LogRecord record) {
        return toChangelogRowData(record);
    }

    @Override
    public org.apache.flink.table.types.logical.RowType getProducedType() {
        return producedType;
    }

    /** Converts a Fluss ChangeType to its string representation for the changelog virtual table. */
    private String convertChangeTypeToString(ChangeType changeType) {
        // Use the short string representation from ChangeType
        return changeType.shortString();
    }

    /**
     * Builds the Flink RowType for the changelog virtual table by adding metadata columns.
     *
     * @param originalType The original table's row type
     * @return The row type with metadata columns prepended
     */
    public static org.apache.flink.table.types.logical.RowType buildChangelogRowType(
            org.apache.flink.table.types.logical.RowType originalType) {
        List<org.apache.flink.table.types.logical.RowType.RowField> fields = new ArrayList<>();

        // Add metadata columns first (using centralized constants from TableDescriptor)
        fields.add(
                new org.apache.flink.table.types.logical.RowType.RowField(
                        TableDescriptor.CHANGE_TYPE_COLUMN, new VarCharType(false, 2)));
        fields.add(
                new org.apache.flink.table.types.logical.RowType.RowField(
                        TableDescriptor.LOG_OFFSET_COLUMN, new BigIntType(false)));
        fields.add(
                new org.apache.flink.table.types.logical.RowType.RowField(
                        TableDescriptor.COMMIT_TIMESTAMP_COLUMN,
                        new LocalZonedTimestampType(false, 3)));

        // Add all original fields
        fields.addAll(originalType.getFields());

        return new org.apache.flink.table.types.logical.RowType(fields);
    }
}
