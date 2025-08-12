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

package com.alibaba.fluss.lake.paimon.source;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.paimon.PaimonLakeStorage;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.flink.types.Row;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimon;

/** Base class for paimon lakehouse test. */
class PaimonSourceTestBase {
    protected static final String DEFAULT_DB = "fluss_lakehouse";
    protected static final String DEFAULT_TABLE = "test_lakehouse_table";
    protected static final int DEFAULT_BUCKET_NUM = 1;

    private static @TempDir File tempWarehouseDir;
    protected static PaimonLakeStorage lakeStorage;
    protected static Catalog paimonCatalog;

    @BeforeAll
    protected static void beforeAll() {
        Configuration configuration = new Configuration();
        configuration.setString("type", "paimon");
        configuration.setString("warehouse", tempWarehouseDir.toString());
        lakeStorage = new PaimonLakeStorage(configuration);
        paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(configuration.toMap())));
    }

    public void createTable(TablePath tablePath, Schema schema) throws Exception {
        paimonCatalog.createDatabase(tablePath.getDatabaseName(), true);
        paimonCatalog.createTable(toPaimon(tablePath), schema, true);
    }

    public void writeRecord(TablePath tablePath, List<InternalRow> records) throws Exception {
        Table table = getTable(tablePath);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite writer = writeBuilder.newWrite()) {
            for (InternalRow record : records) {
                writer.write(record);
            }
            List<CommitMessage> messages = writer.prepareCommit();
            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                commit.commit(messages);
            }
        }
    }

    public Table getTable(TablePath tablePath) throws Exception {
        return paimonCatalog.getTable(toPaimon(tablePath));
    }

    public static List<Row> convertToFlinkRow(
            com.alibaba.fluss.row.InternalRow.FieldGetter[] fieldGetters,
            CloseableIterator<com.alibaba.fluss.row.InternalRow> flussRowIterator) {
        List<Row> rows = new ArrayList<>();
        while (flussRowIterator.hasNext()) {
            com.alibaba.fluss.row.InternalRow row = flussRowIterator.next();
            Row flinkRow = new Row(fieldGetters.length);
            for (int i = 0; i < fieldGetters.length; i++) {
                flinkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
            }
            rows.add(flinkRow);
        }
        return rows;
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
