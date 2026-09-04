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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.lake.source.SortedRecordReader;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkState;

/** Sorted Iceberg reader used by primary-key lake union reads. */
public class IcebergSortedRecordReader implements SortedRecordReader {

    private final @Nullable IcebergRecordReader delegate;
    private final List<Types.NestedField> keyFields;
    private final int[] keyPositions;

    /**
     * Creates an Iceberg sorted reader.
     *
     * @param table Iceberg table to read
     * @param split file split, or null when only the comparator is required
     * @param project top-level projection, or null for the full table projection
     */
    public IcebergSortedRecordReader(
            Table table, @Nullable IcebergSplit split, @Nullable int[][] project) {
        this.delegate =
                split == null
                        ? null
                        : new IcebergRecordReader(split.fileScanTask(), table, project);
        SortOrder sortOrder = createSortOrder(table.schema(), project);
        this.keyFields = sortOrder.keyFields;
        this.keyPositions = sortOrder.keyPositions;
    }

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        if (delegate == null) {
            return CloseableIterator.wrap(Collections.emptyIterator());
        }

        List<LogRecord> records = new ArrayList<>();
        CloseableIterator<LogRecord> iterator = delegate.read();
        try {
            while (iterator.hasNext()) {
                records.add(iterator.next());
            }
        } finally {
            iterator.close();
        }
        records.sort((record1, record2) -> compareRows(record1.getRow(), record2.getRow()));
        return CloseableIterator.wrap(records.iterator());
    }

    @Override
    public Comparator<InternalRow> order() {
        return this::compareKeyRows;
    }

    private int compareRows(InternalRow row1, InternalRow row2) {
        for (int i = 0; i < keyFields.size(); i++) {
            int position = keyPositions[i];
            checkState(
                    !row1.isNullAt(position) && !row2.isNullAt(position),
                    "Iceberg identifier field at position %s must not be null.",
                    position);
            int result = compareValue(row1, row2, position, keyFields.get(i).type());
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    /**
     * Compares rows containing only primary-key fields.
     *
     * <p>The client-side sort/merge reader passes primary-key projections to {@link #order()}, so
     * their positions always start at zero regardless of the positions in the Iceberg table.
     */
    private int compareKeyRows(InternalRow row1, InternalRow row2) {
        for (int i = 0; i < keyFields.size(); i++) {
            checkState(
                    !row1.isNullAt(i) && !row2.isNullAt(i),
                    "Iceberg identifier field at key position %s must not be null.",
                    i);
            int result = compareValue(row1, row2, i, keyFields.get(i).type());
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private int compareValue(InternalRow row1, InternalRow row2, int position, Type type) {
        switch (type.typeId()) {
            case BOOLEAN:
                return Boolean.compare(row1.getBoolean(position), row2.getBoolean(position));
            case INTEGER:
            case DATE:
            case TIME:
                return Integer.compare(row1.getInt(position), row2.getInt(position));
            case LONG:
                return Long.compare(row1.getLong(position), row2.getLong(position));
            case FLOAT:
                return Float.compare(row1.getFloat(position), row2.getFloat(position));
            case DOUBLE:
                return Double.compare(row1.getDouble(position), row2.getDouble(position));
            case STRING:
                return row1.getString(position).compareTo(row2.getString(position));
            case DECIMAL:
                Types.DecimalType decimalType = (Types.DecimalType) type;
                return row1.getDecimal(position, decimalType.precision(), decimalType.scale())
                        .compareTo(
                                row2.getDecimal(
                                        position, decimalType.precision(), decimalType.scale()));
            case TIMESTAMP:
                return compareTimestamp(
                        row1, row2, position, ((Types.TimestampType) type).shouldAdjustToUTC());
            case TIMESTAMP_NANO:
                return compareTimestamp(
                        row1, row2, position, ((Types.TimestampNanoType) type).shouldAdjustToUTC());
            case BINARY:
            case FIXED:
                return compareBytes(row1.getBytes(position), row2.getBytes(position));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Iceberg identifier type: " + type.typeId());
        }
    }

    private int compareTimestamp(
            InternalRow row1, InternalRow row2, int position, boolean shouldAdjustToUTC) {
        if (shouldAdjustToUTC) {
            return row1.getTimestampLtz(position, 6).compareTo(row2.getTimestampLtz(position, 6));
        }
        return row1.getTimestampNtz(position, 6).compareTo(row2.getTimestampNtz(position, 6));
    }

    private int compareBytes(byte[] bytes1, byte[] bytes2) {
        int length = Math.min(bytes1.length, bytes2.length);
        for (int i = 0; i < length; i++) {
            int result = Byte.compare(bytes1[i], bytes2[i]);
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(bytes1.length, bytes2.length);
    }

    private static SortOrder createSortOrder(Schema schema, @Nullable int[][] project) {
        List<Types.NestedField> keyFields = new ArrayList<>();
        List<Integer> originalPositions = new ArrayList<>();
        List<Types.NestedField> columns = schema.columns();
        for (int i = 0; i < columns.size(); i++) {
            Types.NestedField field = columns.get(i);
            if (schema.identifierFieldIds().contains(field.fieldId())) {
                keyFields.add(field);
                originalPositions.add(i);
            }
        }

        int[] keyPositions = new int[keyFields.size()];
        for (int i = 0; i < originalPositions.size(); i++) {
            int position = findProjectedPosition(originalPositions.get(i), project);
            checkState(
                    position >= 0,
                    "Iceberg identifier field at position %s is missing from the projection.",
                    originalPositions.get(i));
            keyPositions[i] = position;
        }
        return new SortOrder(keyFields, keyPositions);
    }

    private static int findProjectedPosition(int originalPosition, @Nullable int[][] project) {
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

    private static final class SortOrder {
        private final List<Types.NestedField> keyFields;
        private final int[] keyPositions;

        private SortOrder(List<Types.NestedField> keyFields, int[] keyPositions) {
            this.keyFields = keyFields;
            this.keyPositions = keyPositions;
        }
    }
}
