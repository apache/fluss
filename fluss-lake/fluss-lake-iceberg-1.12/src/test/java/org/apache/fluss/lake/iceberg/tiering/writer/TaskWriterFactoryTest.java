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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for the Iceberg 1.12 {@link TaskWriterFactory}. */
class TaskWriterFactoryTest {

    /** A primary key table with a non-key column. */
    private static final Schema PK_SCHEMA =
            new Schema(
                    Arrays.asList(
                            required(1, "id", Types.IntegerType.get()),
                            optional(2, "name", Types.StringType.get())),
                    Collections.singleton(1));

    private InMemoryCatalog catalog;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize(
                "test_catalog",
                Collections.singletonMap(CatalogProperties.WAREHOUSE_LOCATION, "/warehouse"));
        catalog.createNamespace(Namespace.of("test_db"));
    }

    @AfterEach
    void tearDown() throws IOException {
        catalog.close();
    }

    @Test
    void testWriteLogTable() throws IOException {
        Schema schema =
                new Schema(
                        optional(1, "id", Types.IntegerType.get()),
                        optional(2, "name", Types.StringType.get()));
        Table table = createTable("log_table", schema);

        try (TaskWriter<Record> writer = TaskWriterFactory.createTaskWriter(table, null, 0)) {
            writer.write(record(schema, 1, "a"));
            writer.write(record(schema, 2, "b"));
            WriteResult result = writer.complete();

            assertThat(result.dataFiles()).hasSize(1);
            assertThat(result.dataFiles()[0].recordCount()).isEqualTo(2);
            assertThat(result.deleteFiles()).isEmpty();
        }
    }

    @Test
    void testWritePrimaryKeyTable() throws IOException {
        // parquet collects column metrics, which show the columns of the delete file
        Table table = createTable("pk_table", PK_SCHEMA, "parquet");

        try (GenericRecordDeltaWriter writer =
                (GenericRecordDeltaWriter) TaskWriterFactory.createTaskWriter(table, null, 0)) {
            writer.write(record(PK_SCHEMA, 1, "a"));
            // a key that was not written by this writer is deleted with an equality delete
            writer.delete(record(PK_SCHEMA, 2, "b"));
            WriteResult result = writer.complete();

            assertThat(result.dataFiles()).hasSize(1);
            assertThat(result.deleteFiles()).hasSize(1);
            DeleteFile deleteFile = result.deleteFiles()[0];
            assertThat(deleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES);
            assertThat(deleteFile.equalityFieldIds()).containsExactly(1);
            // the equality delete file contains the full row, not only the key
            assertThat(deleteFile.valueCounts()).containsOnlyKeys(1, 2);
        }
    }

    @Test
    void testDeleteRowWrittenBySameWriter() throws IOException {
        Table table = createTable("pk_table_same_writer", PK_SCHEMA, "parquet");

        try (GenericRecordDeltaWriter writer =
                (GenericRecordDeltaWriter) TaskWriterFactory.createTaskWriter(table, null, 0)) {
            writer.write(record(PK_SCHEMA, 1, "a"));
            // a key written by this writer is deleted with a position delete
            writer.delete(record(PK_SCHEMA, 1, "a"));
            WriteResult result = writer.complete();

            assertThat(result.dataFiles()).hasSize(1);
            assertThat(result.deleteFiles()).hasSize(1);
            assertThat(result.deleteFiles()[0].content()).isEqualTo(FileContent.POSITION_DELETES);
            assertThat(result.deleteFiles()[0].recordCount()).isEqualTo(1);
        }
    }

    private Table createTable(String name, Schema schema) {
        return createTable(name, schema, "avro");
    }

    private Table createTable(String name, Schema schema, String fileFormat) {
        return catalog.createTable(
                TableIdentifier.of("test_db", name),
                schema,
                PartitionSpec.unpartitioned(),
                Collections.singletonMap(TableProperties.DEFAULT_FILE_FORMAT, fileFormat));
    }

    private static Record record(Schema schema, Object... values) {
        GenericRecord record = GenericRecord.create(schema);
        for (int i = 0; i < values.length; i++) {
            record.set(i, values[i]);
        }
        return record;
    }
}
