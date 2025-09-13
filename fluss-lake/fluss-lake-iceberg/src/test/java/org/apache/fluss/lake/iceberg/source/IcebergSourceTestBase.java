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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.IcebergLakeStorage;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.lake.iceberg.utils.IcebergCatalogUtils;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkState;

/** Base class for Iceberg source tests. */
class IcebergSourceTestBase {
    protected static final String DEFAULT_DB = "fluss_lakehouse";
    protected static final String DEFAULT_TABLE = "test_lakehouse_table";
    protected static final int DEFAULT_BUCKET_NUM = 1;

    private static @TempDir File tempWarehouseDir;
    protected static IcebergLakeStorage lakeStorage;
    protected static Catalog icebergCatalog;

    @BeforeAll
    protected static void beforeAll() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir.toString());
        configuration.setString("type", "hadoop");
        configuration.setString("name", "fluss_test_catalog");
        lakeStorage = new IcebergLakeStorage(configuration);
        icebergCatalog = IcebergCatalogUtils.createIcebergCatalog(configuration);
    }

    public void createTable(TablePath tablePath, Schema schema, PartitionSpec partitionSpec)
            throws Exception {
        if (!((SupportsNamespaces) icebergCatalog)
                .namespaceExists(Namespace.of(tablePath.getDatabaseName()))) {
            ((SupportsNamespaces) icebergCatalog)
                    .createNamespace(Namespace.of(tablePath.getDatabaseName()));
        }
        icebergCatalog.createTable(
                TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName()),
                schema,
                partitionSpec);
    }

    public void writeRecord(
            Table table, List<Record> records, @Nullable String partition, int bucket)
            throws Exception {
        try (TaskWriter<Record> taskWriter =
                TaskWriterFactory.createTaskWriter(table, partition, bucket)) {
            for (Record r : records) {
                taskWriter.write(r);
            }
            DataFile[] dataFiles = taskWriter.dataFiles();
            checkState(dataFiles.length == 1);
            table.newAppend().appendFile(dataFiles[0]).commit();
        }
    }

    public static List<Row> convertToFlinkRow(
            org.apache.fluss.row.InternalRow.FieldGetter[] fieldGetters,
            CloseableIterator<InternalRow> flussRowIterator) {
        List<Row> rows = new ArrayList<>();
        while (flussRowIterator.hasNext()) {
            org.apache.fluss.row.InternalRow row = flussRowIterator.next();
            Row flinkRow = new Row(fieldGetters.length);
            for (int i = 0; i < fieldGetters.length; i++) {
                flinkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
            }
            rows.add(flinkRow);
        }
        return rows;
    }

    public Table getTable(TablePath tablePath) throws Exception {
        return icebergCatalog.loadTable(
                TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName()));
    }

    public GenericRecord createIcebergRecord(Schema schema, Object... values) {
        GenericRecord record = GenericRecord.create(schema);
        for (int i = 0; i < values.length; i++) {
            record.set(i, values[i]);
        }
        return record;
    }

    /** Adapter for transforming closeable iterator. */
    public static class TransformingCloseableIterator<T, U> implements CloseableIterator<U> {
        private final CloseableIterator<T> source;
        private final Function<? super T, ? extends U> transformer;

        public TransformingCloseableIterator(
                CloseableIterator<T> source, Function<? super T, ? extends U> transformer) {
            this.source = source;
            this.transformer = transformer;
        }

        @Override
        public boolean hasNext() {
            return source.hasNext();
        }

        @Override
        public U next() {
            return transformer.apply(source.next());
        }

        @Override
        public void close() {
            source.close();
        }

        public static <T, U> CloseableIterator<U> transform(
                CloseableIterator<T> source, Function<? super T, ? extends U> transformer) {
            return new TransformingCloseableIterator<>(source, transformer);
        }
    }
}
