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

package org.apache.fluss.lake.lance.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lance.LanceConfig;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.metadata.TablePath;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.ipc.LanceScanner;
import com.lancedb.lance.ipc.ScanOptions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** Record reader for Lance table split. */
public class LanceRecordReader implements RecordReader {
    private final LanceConfig lanceConfig;
    private final LanceSplit split;
    private final @Nullable int[][] project;
    private final @Nullable String filterSql;

    public LanceRecordReader(
            Configuration configuration,
            TablePath tablePath,
            LanceSplit split,
            @Nullable int[][] project,
            @Nullable String filterSql) {
        this.lanceConfig =
                LanceConfig.from(
                        configuration.toMap(),
                        Collections.emptyMap(),
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());
        this.split = split;
        this.project = project;
        this.filterSql = filterSql;
    }

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        if (split.scanLimit() == 0) {
            return CloseableIterator.emptyIterator();
        }

        ReadOptions readOptions = LanceConfig.genReadOptionFromConfig(lanceConfig);
        Dataset dataset = Dataset.open(lanceConfig.getDatasetUri(), readOptions);
        if (split.snapshotId() > 0) {
            dataset.checkoutVersion(split.snapshotId());
        }

        List<String> dataColumns = listDataColumns(dataset);
        List<String> outputColumns = resolveOutputColumns(dataColumns, project);
        List<String> scanColumns = buildScanColumns(outputColumns);

        ScanOptions.Builder optionsBuilder =
                new ScanOptions.Builder().fragmentIds(Arrays.asList(split.fragmentId()));
        if (!scanColumns.isEmpty()) {
            optionsBuilder.columns(scanColumns);
        }
        if (filterSql != null && !filterSql.isEmpty()) {
            optionsBuilder.filter(filterSql);
        }
        if (split.scanLimit() > 0) {
            optionsBuilder.limit(split.scanLimit());
        }

        LanceScanner scanner = dataset.newScan(optionsBuilder.build());
        ArrowReader arrowReader = scanner.scanBatches();

        return new LanceRecordIterator(dataset, scanner, arrowReader, outputColumns);
    }

    private static List<String> listDataColumns(Dataset dataset) {
        List<String> columns = new ArrayList<String>();
        for (Field field : dataset.getSchema().getFields()) {
            if (isSystemColumn(field.getName())) {
                continue;
            }
            columns.add(field.getName());
        }
        return columns;
    }

    private static List<String> resolveOutputColumns(List<String> dataColumns, @Nullable int[][] project) {
        if (project == null) {
            return dataColumns;
        }

        List<String> projected = new ArrayList<String>();
        for (int[] indexPath : project) {
            if (indexPath == null || indexPath.length == 0) {
                continue;
            }
            int topLevelIndex = indexPath[0];
            if (topLevelIndex < 0 || topLevelIndex >= dataColumns.size()) {
                continue;
            }
            projected.add(dataColumns.get(topLevelIndex));
        }
        return projected;
    }

    private static List<String> buildScanColumns(List<String> outputColumns) {
        Set<String> columns = new LinkedHashSet<String>(outputColumns);
        columns.add(OFFSET_COLUMN_NAME);
        columns.add(TIMESTAMP_COLUMN_NAME);
        return new ArrayList<String>(columns);
    }

    private static boolean isSystemColumn(String name) {
        return OFFSET_COLUMN_NAME.equals(name)
                || TIMESTAMP_COLUMN_NAME.equals(name)
                || BUCKET_COLUMN_NAME.equals(name);
    }

    private static final class LanceRecordIterator implements CloseableIterator<LogRecord> {
        private final Dataset dataset;
        private final LanceScanner scanner;
        private final ArrowReader arrowReader;
        private final List<String> outputColumns;

        private VectorSchemaRoot currentBatch;
        private int rowIndexInBatch;

        private LanceRecordIterator(
                Dataset dataset,
                LanceScanner scanner,
                ArrowReader arrowReader,
                List<String> outputColumns) {
            this.dataset = dataset;
            this.scanner = scanner;
            this.arrowReader = arrowReader;
            this.outputColumns = outputColumns;
            this.currentBatch = null;
            this.rowIndexInBatch = 0;
        }

        @Override
        public boolean hasNext() {
            try {
                while (currentBatch == null || rowIndexInBatch >= currentBatch.getRowCount()) {
                    if (!arrowReader.loadNextBatch()) {
                        return false;
                    }
                    currentBatch = arrowReader.getVectorSchemaRoot();
                    rowIndexInBatch = 0;
                }
                return true;
            } catch (IOException e) {
                throw new RuntimeException("Failed to read Lance scan batch.", e);
            }
        }

        @Override
        public LogRecord next() {
            GenericRow row = new GenericRow(outputColumns.size());
            for (int i = 0; i < outputColumns.size(); i++) {
                FieldVector vector = currentBatch.getVector(outputColumns.get(i));
                row.setField(i, vectorToInternalValue(vector, rowIndexInBatch));
            }

            long offset = getLong(currentBatch.getVector(OFFSET_COLUMN_NAME), rowIndexInBatch, -1L);
            long timestamp =
                    getLong(currentBatch.getVector(TIMESTAMP_COLUMN_NAME), rowIndexInBatch, System.currentTimeMillis());

            rowIndexInBatch++;
            return new GenericRecord(offset, timestamp, ChangeType.APPEND_ONLY, row);
        }

        @Override
        public void close() {
            closeQuietly(arrowReader);
            closeQuietly(scanner);
            closeQuietly(dataset);
        }

        private static long getLong(@Nullable ValueVector vector, int row, long defaultValue) {
            if (vector == null || vector.isNull(row)) {
                return defaultValue;
            }
            if (vector instanceof BigIntVector) {
                return ((BigIntVector) vector).get(row);
            }
            Object value = vector.getObject(row);
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            return defaultValue;
        }

        private static Object vectorToInternalValue(@Nullable ValueVector vector, int row) {
            if (vector == null || vector.isNull(row)) {
                return null;
            }

            if (vector instanceof IntVector) {
                return ((IntVector) vector).get(row);
            }
            if (vector instanceof BigIntVector) {
                return ((BigIntVector) vector).get(row);
            }
            if (vector instanceof TinyIntVector) {
                return ((TinyIntVector) vector).get(row);
            }
            if (vector instanceof SmallIntVector) {
                return ((SmallIntVector) vector).get(row);
            }
            if (vector instanceof Float4Vector) {
                return ((Float4Vector) vector).get(row);
            }
            if (vector instanceof Float8Vector) {
                return ((Float8Vector) vector).get(row);
            }
            if (vector instanceof BitVector) {
                return ((BitVector) vector).get(row) == 1;
            }
            if (vector instanceof VarCharVector) {
                return BinaryString.fromString(((VarCharVector) vector).getObject(row).toString());
            }
            if (vector instanceof VarBinaryVector) {
                return ((VarBinaryVector) vector).get(row);
            }
            if (vector instanceof FixedSizeBinaryVector) {
                return ((FixedSizeBinaryVector) vector).get(row);
            }
            if (vector instanceof DecimalVector) {
                DecimalVector decimalVector = (DecimalVector) vector;
                BigDecimal value = decimalVector.getObject(row);
                return Decimal.fromBigDecimal(value, decimalVector.getPrecision(), decimalVector.getScale());
            }

            Object value = vector.getObject(row);
            if (value instanceof Text) {
                return BinaryString.fromString(value.toString());
            }
            if (value instanceof String) {
                return BinaryString.fromString((String) value);
            }
            return value;
        }

        private static void closeQuietly(Object resource) {
            if (resource == null) {
                return;
            }
            try {
                if (resource instanceof AutoCloseable) {
                    ((AutoCloseable) resource).close();
                }
            } catch (Exception e) {
                // best effort close
            }
        }
    }
}
