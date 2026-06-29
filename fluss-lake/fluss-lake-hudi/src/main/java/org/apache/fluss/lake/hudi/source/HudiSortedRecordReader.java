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

package org.apache.fluss.lake.hudi.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.hudi.utils.HudiTableInfo;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.SortedRecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.InternalRowUtils;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.ExpressionPredicates;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.hudi.HudiLakeCatalog.SYSTEM_COLUMNS;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Sorted Hudi record reader for primary key table union read. */
public class HudiSortedRecordReader implements SortedRecordReader {

    private static final String DELIMITER = ",";

    private final @Nullable RecordReader delegate;
    private final SortOrder sortOrder;

    public HudiSortedRecordReader(
            Configuration hudiConfig,
            TablePath tablePath,
            @Nullable HudiSplit hudiSplit,
            @Nullable int[][] project,
            List<ExpressionPredicates.Predicate> predicates)
            throws Exception {
        this.delegate =
                hudiSplit == null
                        ? null
                        : new HudiRecordReader(
                                hudiConfig, tablePath, hudiSplit, project, predicates);
        this.sortOrder = createSortOrder(hudiConfig, tablePath, project);
    }

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        if (delegate == null) {
            return CloseableIterator.wrap(Collections.emptyIterator());
        }
        CloseableIterator<LogRecord> iterator = delegate.read();
        // TODO: Introduce a spillable sorter for large Hudi splits to avoid keeping all records
        // in memory while preserving the SortedRecordReader order contract.
        List<LogRecord> records = new ArrayList<>();
        try {
            while (iterator.hasNext()) {
                // HudiRecordReader reuses its projected row wrapper, so records must be copied
                // before they are buffered for sorting.
                records.add(copyRecord(iterator.next(), sortOrder.producedTypes));
            }
        } finally {
            iterator.close();
        }
        records.sort(
                (record1, record2) ->
                        compareRows(
                                record1.getRow(),
                                record2.getRow(),
                                sortOrder.keyPositionsInRecord,
                                sortOrder.keyTypes));
        return CloseableIterator.wrap(records.iterator());
    }

    @Override
    public Comparator<InternalRow> order() {
        return (row1, row2) ->
                compareRows(row1, row2, sortOrder.keyPositionsInKey, sortOrder.keyTypes);
    }

    static boolean canSortByRecordKey(HudiTableInfo hudiTableInfo, @Nullable int[][] project) {
        RecordKeyInfo recordKeyInfo = resolveRecordKeyInfo(hudiTableInfo);
        if (project == null) {
            return true;
        }
        for (int userFieldPosition : recordKeyInfo.keyUserFieldPositions) {
            if (findProducedPosition(userFieldPosition, project) < 0) {
                return false;
            }
        }
        return true;
    }

    private static SortOrder createSortOrder(
            Configuration hudiConfig, TablePath tablePath, @Nullable int[][] project)
            throws IOException {
        try (HudiTableInfo hudiTableInfo = HudiTableInfo.create(tablePath, hudiConfig)) {
            return resolveSortOrder(hudiTableInfo, project);
        }
    }

    private static SortOrder resolveSortOrder(
            HudiTableInfo hudiTableInfo, @Nullable int[][] project) {
        RecordKeyInfo recordKeyInfo = resolveRecordKeyInfo(hudiTableInfo);
        int[] keyPositions = new int[recordKeyInfo.keyUserFieldPositions.length];
        for (int i = 0; i < recordKeyInfo.keyUserFieldPositions.length; i++) {
            int keyPosition = findProducedPosition(recordKeyInfo.keyUserFieldPositions[i], project);
            if (keyPosition < 0) {
                throw new IllegalArgumentException(
                        "Can not find Hudi record key field in projected fields.");
            }
            keyPositions[i] = keyPosition;
        }
        return new SortOrder(
                recordKeyInfo.keyTypes,
                keyPositions,
                producedTypes(recordKeyInfo.userDataTypes, project));
    }

    private static RecordKeyInfo resolveRecordKeyInfo(HudiTableInfo hudiTableInfo) {
        String recordKeyFields =
                hudiTableInfo.getTableOptions().get(FlinkOptions.RECORD_KEY_FIELD.key());
        if (recordKeyFields == null || recordKeyFields.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Hudi record key field is required for sorted primary key table reader.");
        }

        Map<String, DataType> dataTypesByName = new HashMap<>();
        Map<String, Integer> userFieldPositionsByName = new HashMap<>();
        List<DataType> userDataTypes = new ArrayList<>();
        int userFieldPosition = 0;
        for (Schema.UnresolvedColumn column :
                hudiTableInfo.getHudiTable().getUnresolvedSchema().getColumns()) {
            if (SYSTEM_COLUMNS.containsKey(column.getName())) {
                continue;
            }
            DataType dataType = getDataType(column);
            dataTypesByName.put(column.getName(), dataType);
            userFieldPositionsByName.put(column.getName(), userFieldPosition++);
            userDataTypes.add(dataType);
        }

        List<String> keyFields =
                Arrays.stream(recordKeyFields.split(DELIMITER))
                        .map(String::trim)
                        .filter(key -> !key.isEmpty())
                        .collect(Collectors.toList());

        List<DataType> keyTypes = new ArrayList<>(keyFields.size());
        int[] keyUserFieldPositions = new int[keyFields.size()];
        for (int i = 0; i < keyFields.size(); i++) {
            String key = keyFields.get(i);
            DataType dataType = dataTypesByName.get(key);
            Integer keyUserFieldPosition = userFieldPositionsByName.get(key);
            if (dataType == null || keyUserFieldPosition == null) {
                throw new IllegalArgumentException(
                        "Can not find Hudi record key field " + key + ".");
            }
            keyTypes.add(dataType);
            keyUserFieldPositions[i] = keyUserFieldPosition;
        }
        return new RecordKeyInfo(userDataTypes, keyTypes, keyUserFieldPositions);
    }

    private static DataType getDataType(Schema.UnresolvedColumn column) {
        AbstractDataType<?> dataType;
        if (column instanceof Schema.UnresolvedPhysicalColumn) {
            dataType = ((Schema.UnresolvedPhysicalColumn) column).getDataType();
        } else if (column instanceof Schema.UnresolvedMetadataColumn) {
            dataType = ((Schema.UnresolvedMetadataColumn) column).getDataType();
        } else {
            throw new IllegalStateException("Unexpected column kind: " + column.getClass());
        }
        if (dataType instanceof DataType) {
            return (DataType) dataType;
        }
        throw new IllegalArgumentException("Unsupported Hudi column data type: " + dataType);
    }

    private static int findProducedPosition(int originalPosition, @Nullable int[][] project) {
        if (project == null) {
            return originalPosition;
        }
        for (int i = 0; i < project.length; i++) {
            if (project[i].length > 0 && project[i][0] == originalPosition) {
                return i;
            }
        }
        return -1;
    }

    private static List<DataType> producedTypes(
            List<DataType> userDataTypes, @Nullable int[][] project) {
        if (project == null) {
            return userDataTypes;
        }
        List<DataType> producedTypes = new ArrayList<>(project.length);
        for (int[] projectPath : project) {
            if (projectPath.length > 0) {
                producedTypes.add(userDataTypes.get(projectPath[0]));
            }
        }
        return producedTypes;
    }

    private static LogRecord copyRecord(LogRecord record, List<DataType> producedTypes) {
        return new GenericRecord(
                record.logOffset(),
                record.timestamp(),
                record.getChangeType(),
                copyRow(record.getRow(), producedTypes));
    }

    private static InternalRow copyRow(InternalRow row, List<DataType> fieldTypes) {
        GenericRow copiedRow = new GenericRow(fieldTypes.size());
        for (int i = 0; i < fieldTypes.size(); i++) {
            if (row.isNullAt(i)) {
                copiedRow.setField(i, null);
            } else {
                copiedRow.setField(i, copyField(row, i, fieldTypes.get(i).getLogicalType()));
            }
        }
        return copiedRow;
    }

    private static Object copyField(InternalRow row, int pos, LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(pos);
            case TINYINT:
                return row.getByte(pos);
            case SMALLINT:
                return row.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return row.getInt(pos);
            case BIGINT:
                return row.getLong(pos);
            case FLOAT:
                return row.getFloat(pos);
            case DOUBLE:
                return row.getDouble(pos);
            case CHAR:
            case VARCHAR:
                return row.getString(pos).copy();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                Decimal decimal =
                        row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
                return decimal.copy();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) logicalType;
                return row.getTimestampNtz(pos, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType timestampLtzType = (LocalZonedTimestampType) logicalType;
                return row.getTimestampLtz(pos, timestampLtzType.getPrecision());
            case BINARY:
            case VARBINARY:
                byte[] bytes = row.getBytes(pos);
                return Arrays.copyOf(bytes, bytes.length);
            default:
                throw new IllegalArgumentException(
                        "Unsupported Hudi row field type: " + logicalType);
        }
    }

    private static int compareRows(
            InternalRow row1, InternalRow row2, int[] keyPositions, List<DataType> keyTypes) {
        for (int pos = 0; pos < keyTypes.size(); pos++) {
            DataType keyType = keyTypes.get(pos);
            int keyPosition = keyPositions[pos];
            checkState(
                    !row1.isNullAt(keyPosition) && !row2.isNullAt(keyPosition),
                    "Hudi record key field at position %s must not be null.",
                    keyPosition);
            int result =
                    compareValues(
                            getField(row1, keyPosition, keyType),
                            getField(row2, keyPosition, keyType),
                            keyType.getLogicalType());
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private static Object getField(InternalRow row, int pos, DataType dataType) {
        LogicalType logicalType = dataType.getLogicalType();
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(pos);
            case TINYINT:
                return row.getByte(pos);
            case SMALLINT:
                return row.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return row.getInt(pos);
            case BIGINT:
                return row.getLong(pos);
            case FLOAT:
                return row.getFloat(pos);
            case DOUBLE:
                return row.getDouble(pos);
            case CHAR:
            case VARCHAR:
                return row.getString(pos);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                return row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) logicalType;
                return row.getTimestampNtz(pos, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType timestampLtzType = (LocalZonedTimestampType) logicalType;
                return row.getTimestampLtz(pos, timestampLtzType.getPrecision());
            case BINARY:
            case VARBINARY:
                return row.getBytes(pos);
            default:
                throw new IllegalArgumentException(
                        "Unsupported Hudi primary key type: " + logicalType);
        }
    }

    private static int compareValues(Object value1, Object value2, LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return Boolean.compare((boolean) value1, (boolean) value2);
            case CHAR:
            case VARCHAR:
                return ((BinaryString) value1).compareTo((BinaryString) value2);
            case VARBINARY:
                return InternalRowUtils.compare(value1, value2, DataTypeRoot.BYTES);
            default:
                return InternalRowUtils.compare(
                        value1, value2, toFlussDataTypeRoot(logicalType.getTypeRoot()));
        }
    }

    private static DataTypeRoot toFlussDataTypeRoot(
            org.apache.flink.table.types.logical.LogicalTypeRoot logicalTypeRoot) {
        switch (logicalTypeRoot) {
            case TINYINT:
                return DataTypeRoot.TINYINT;
            case SMALLINT:
                return DataTypeRoot.SMALLINT;
            case INTEGER:
                return DataTypeRoot.INTEGER;
            case BIGINT:
                return DataTypeRoot.BIGINT;
            case FLOAT:
                return DataTypeRoot.FLOAT;
            case DOUBLE:
                return DataTypeRoot.DOUBLE;
            case DECIMAL:
                return DataTypeRoot.DECIMAL;
            case DATE:
                return DataTypeRoot.DATE;
            case TIME_WITHOUT_TIME_ZONE:
                return DataTypeRoot.TIME_WITHOUT_TIME_ZONE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
            case BINARY:
                return DataTypeRoot.BINARY;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Hudi primary key type: " + logicalTypeRoot);
        }
    }

    private static class SortOrder {
        private final List<DataType> keyTypes;
        private final int[] keyPositionsInRecord;
        private final int[] keyPositionsInKey;
        private final List<DataType> producedTypes;

        private SortOrder(
                List<DataType> keyTypes, int[] keyPositionsInRecord, List<DataType> producedTypes) {
            this.keyTypes = keyTypes;
            this.keyPositionsInRecord = keyPositionsInRecord;
            this.producedTypes = producedTypes;
            this.keyPositionsInKey = new int[keyTypes.size()];
            for (int i = 0; i < keyTypes.size(); i++) {
                this.keyPositionsInKey[i] = i;
            }
        }
    }

    private static class RecordKeyInfo {
        private final List<DataType> userDataTypes;
        private final List<DataType> keyTypes;
        private final int[] keyUserFieldPositions;

        private RecordKeyInfo(
                List<DataType> userDataTypes,
                List<DataType> keyTypes,
                int[] keyUserFieldPositions) {
            this.userDataTypes = userDataTypes;
            this.keyTypes = keyTypes;
            this.keyUserFieldPositions = keyUserFieldPositions;
        }
    }
}
