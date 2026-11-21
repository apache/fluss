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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests that tiering writers honor Iceberg table properties. */
class TaskWriterFactoryTest {

    private static final Schema LOG_SCHEMA =
            new Schema(
                    required(1, "id", Types.IntegerType.get()),
                    optional(2, "value", Types.StringType.get()));

    private static final Schema PK_SCHEMA =
            new Schema(LOG_SCHEMA.columns(), Collections.singleton(1));

    @TempDir private File tempDir;

    @Test
    void testWriteLogTableWithTableProperties() throws IOException {
        Table table = createTable("log_table", LOG_SCHEMA);

        WriteResult result;
        try (TaskWriter<Record> writer = TaskWriterFactory.createTaskWriter(table, null, 0)) {
            writer.write(record(LOG_SCHEMA, 1, "a"));
            writer.write(record(LOG_SCHEMA, 2, "b"));
            result = writer.complete();
        }

        assertThat(result.dataFiles()).hasSize(1);
        DataFile dataFile = result.dataFiles()[0];
        assertThat(dataFile.recordCount()).isEqualTo(2);
        assertThat(result.deleteFiles()).isEmpty();
        assertZstdCompression(dataFile.location());
        assertThat(dataFile.lowerBounds()).containsOnlyKeys(1);
        assertThat(dataFile.upperBounds()).containsOnlyKeys(1);
    }

    @Test
    void testWritePrimaryKeyTableWithTableProperties() throws IOException {
        Table table = createTable("pk_table", PK_SCHEMA);

        WriteResult result;
        try (GenericRecordDeltaWriter writer =
                (GenericRecordDeltaWriter) TaskWriterFactory.createTaskWriter(table, null, 0)) {
            writer.write(record(PK_SCHEMA, 1, "a"));
            writer.delete(record(PK_SCHEMA, 2, "b"));
            result = writer.complete();
        }

        assertThat(result.dataFiles()).hasSize(1);
        DataFile dataFile = result.dataFiles()[0];
        assertThat(dataFile.recordCount()).isEqualTo(1);
        assertZstdCompression(dataFile.location());
        assertThat(dataFile.lowerBounds()).containsOnlyKeys(1);
        assertThat(dataFile.upperBounds()).containsOnlyKeys(1);

        assertThat(result.deleteFiles()).hasSize(1);
        DeleteFile deleteFile = result.deleteFiles()[0];
        assertThat(deleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES);
        assertThat(deleteFile.equalityFieldIds()).containsExactly(1);
        assertThat(deleteFile.recordCount()).isEqualTo(1);
        assertZstdCompression(deleteFile.location());
    }

    private Table createTable(String name, Schema schema) {
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, "2");
        properties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
        properties.put(TableProperties.PARQUET_COMPRESSION, "zstd");
        properties.put(TableProperties.PARQUET_COMPRESSION_LEVEL, "3");
        properties.put(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "value", "none");
        return new HadoopTables(new Configuration())
                .create(
                        schema,
                        PartitionSpec.unpartitioned(),
                        properties,
                        new File(tempDir, name).toURI().toString());
    }

    private static Record record(Schema schema, Object... values) {
        GenericRecord record = GenericRecord.create(schema);
        for (int i = 0; i < values.length; i++) {
            record.set(i, values[i]);
        }
        return record;
    }

    private static void assertZstdCompression(String file) throws IOException {
        try (ParquetFileReader reader =
                ParquetFileReader.open(
                        HadoopInputFile.fromPath(new Path(file), new Configuration()))) {
            assertThat(reader.getFooter().getBlocks()).isNotEmpty();
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                assertThat(block.getColumns())
                        .extracting(ColumnChunkMetaData::getCodec)
                        .containsOnly(CompressionCodecName.ZSTD);
            }
        }
    }
}
