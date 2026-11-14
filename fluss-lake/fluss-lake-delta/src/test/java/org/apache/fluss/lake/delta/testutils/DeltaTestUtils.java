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

package org.apache.fluss.lake.delta.testutils;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

/** Reads Delta tables back with Delta Kernel to verify what the tiering wrote. */
public class DeltaTestUtils {

    private DeltaTestUtils() {}

    /**
     * Reads all rows of the latest snapshot. Each row is rendered as {@code +I[v1, v2, ...]}, with
     * arrays as {@code [..]}, maps as {@code {k=v}} and structs as {@code (..)}.
     */
    public static List<String> readRows(Engine engine, String tablePath) throws IOException {
        Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(engine);
        Scan scan = snapshot.getScanBuilder().build();
        Row scanState = scan.getScanState(engine);
        StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(scanState);

        List<String> rows = new ArrayList<>();
        try (CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine)) {
            while (scanFiles.hasNext()) {
                try (CloseableIterator<Row> scanFileRows = scanFiles.next().getRows()) {
                    while (scanFileRows.hasNext()) {
                        Row scanFileRow = scanFileRows.next();
                        FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
                        CloseableIterator<ColumnarBatch> physicalData =
                                engine.getParquetHandler()
                                        .readParquetFiles(
                                                singleton(fileStatus),
                                                physicalReadSchema,
                                                Optional.empty())
                                        .map(FileReadResult::getData);
                        try (CloseableIterator<FilteredColumnarBatch> logicalData =
                                Scan.transformPhysicalData(
                                        engine, scanState, scanFileRow, physicalData)) {
                            while (logicalData.hasNext()) {
                                appendRows(logicalData.next(), rows);
                            }
                        }
                    }
                }
            }
        }
        return rows;
    }

    private static void appendRows(FilteredColumnarBatch batch, List<String> rows) {
        ColumnarBatch data = batch.getData();
        Optional<ColumnVector> selection = batch.getSelectionVector();
        for (int rowId = 0; rowId < data.getSize(); rowId++) {
            if (selection.isPresent()
                    && (selection.get().isNullAt(rowId) || !selection.get().getBoolean(rowId))) {
                continue;
            }
            StringJoiner joiner = new StringJoiner(", ", "+I[", "]");
            for (int i = 0; i < data.getSchema().length(); i++) {
                joiner.add(render(data.getColumnVector(i), rowId));
            }
            rows.add(joiner.toString());
        }
    }

    /** Renders one value of a column vector, see {@link #readRows}. */
    public static String render(ColumnVector vector, int rowId) {
        if (vector.isNullAt(rowId)) {
            return "null";
        }
        DataType type = vector.getDataType();
        if (type instanceof BooleanType) {
            return String.valueOf(vector.getBoolean(rowId));
        } else if (type instanceof ByteType) {
            return String.valueOf(vector.getByte(rowId));
        } else if (type instanceof ShortType) {
            return String.valueOf(vector.getShort(rowId));
        } else if (type instanceof IntegerType || type instanceof DateType) {
            return String.valueOf(vector.getInt(rowId));
        } else if (type instanceof LongType
                || type instanceof TimestampType
                || type instanceof TimestampNTZType) {
            return String.valueOf(vector.getLong(rowId));
        } else if (type instanceof FloatType) {
            return String.valueOf(vector.getFloat(rowId));
        } else if (type instanceof DoubleType) {
            return String.valueOf(vector.getDouble(rowId));
        } else if (type instanceof StringType) {
            return vector.getString(rowId);
        } else if (type instanceof BinaryType) {
            return Arrays.toString(vector.getBinary(rowId));
        } else if (type instanceof DecimalType) {
            return vector.getDecimal(rowId).toPlainString();
        } else if (type instanceof ArrayType) {
            ArrayValue array = vector.getArray(rowId);
            StringJoiner joiner = new StringJoiner(", ", "[", "]");
            for (int i = 0; i < array.getSize(); i++) {
                joiner.add(render(array.getElements(), i));
            }
            return joiner.toString();
        } else if (type instanceof MapType) {
            MapValue map = vector.getMap(rowId);
            StringJoiner joiner = new StringJoiner(", ", "{", "}");
            for (int i = 0; i < map.getSize(); i++) {
                joiner.add(render(map.getKeys(), i) + "=" + render(map.getValues(), i));
            }
            return joiner.toString();
        } else if (type instanceof StructType) {
            StringJoiner joiner = new StringJoiner(", ", "(", ")");
            for (int i = 0; i < ((StructType) type).length(); i++) {
                joiner.add(render(vector.getChild(i), rowId));
            }
            return joiner.toString();
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
    }

    private static <T> CloseableIterator<T> singleton(T value) {
        List<T> values = Collections.singletonList(value);
        return new CloseableIterator<T>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < values.size();
            }

            @Override
            public T next() {
                return values.get(index++);
            }

            @Override
            public void close() {}
        };
    }
}
