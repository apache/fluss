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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.fluss.lake.iceberg.tiering.IcebergCommittable;
import org.apache.fluss.lake.iceberg.tiering.IcebergCommittableSerializer;
import org.apache.fluss.lake.iceberg.tiering.IcebergWriteResult;
import org.apache.fluss.lake.iceberg.tiering.IcebergWriteResultSerializer;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/** Tests shared by the Iceberg 1.10 and 1.12 plugins. */
class GenericRecordDVWriterTest {

    private static final Schema SCHEMA =
            new Schema(
                    Arrays.asList(
                            Types.NestedField.required(1, "id", Types.IntegerType.get()),
                            Types.NestedField.optional(2, "value", Types.StringType.get())),
                    Collections.singleton(1));

    private InMemoryCatalog catalog;
    private Table table;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize(
                "test",
                Collections.singletonMap(CatalogProperties.WAREHOUSE_LOCATION, "/warehouse"));
        catalog.createNamespace(Namespace.of("db"));
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, "3");
        properties.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
        // Iceberg checks the size every 1000 records. Force multiple data files in one task.
        properties.put(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "1");
        table =
                catalog.createTable(
                        TableIdentifier.of("db", "pk"),
                        SCHEMA,
                        PartitionSpec.unpartitioned(),
                        properties);
    }

    @AfterEach
    void tearDown() throws IOException {
        catalog.close();
    }

    @Test
    void testRollingDataFilesAndRestoredDeletionVectors() throws Exception {
        WriteResult result;
        try (DeltaWriter writer = newWriter()) {
            for (int i = 0; i < 2000; i++) {
                writer.write(record(i, "initial"));
            }
            writer.write(record(0, "updated"));
            writer.write(record(1999, "updated"));
            writer.delete(record(500, "initial"));
            writer.write(record(500, "reinserted"));
            result = writer.complete();
        }
        assertThat(result.dataFiles()).hasSize(3);
        assertThat(result.deleteFiles()).hasSize(2).allMatch(ContentFileUtil::isDV);
        assertThat(result.referencedDataFiles()).hasSize(2);
        DeleteFile first = result.deleteFiles()[0];
        DeleteFile second = result.deleteFiles()[1];
        assertThat(first.location()).isEqualTo(second.location());
        assertThat(first.contentOffset()).isNotEqualTo(second.contentOffset());
        assertThat(first.referencedDataFile()).isNotEqualTo(second.referencedDataFile());
        assertThat(first.recordCount() + second.recordCount()).isEqualTo(3);

        // Exercise both checkpoint serializer boundaries, including two blobs in one Puffin file.
        IcebergWriteResultSerializer resultSerializer = new IcebergWriteResultSerializer();
        WriteResult restored =
                resultSerializer
                        .deserialize(
                                resultSerializer.getVersion(),
                                resultSerializer.serialize(
                                        new IcebergWriteResult(result, table.specs(), null)))
                        .getWriteResult();
        assertThat(restored.referencedDataFiles())
                .containsExactlyInAnyOrder(result.referencedDataFiles());
        IcebergCommittable.Builder builder = IcebergCommittable.builder().addSpecs(table.specs());
        Arrays.stream(restored.dataFiles()).forEach(builder::addDataFile);
        Arrays.stream(restored.deleteFiles()).forEach(builder::addDeleteFile);
        IcebergCommittableSerializer committableSerializer = new IcebergCommittableSerializer();
        IcebergCommittable committable =
                committableSerializer.deserialize(
                        committableSerializer.getVersion(),
                        committableSerializer.serialize(builder.build()));
        assertThat(committable.getDeleteFiles()).hasSize(2);
        for (DeleteFile delete : committable.getDeleteFiles()) {
            assertThat(delete.format()).isEqualTo(FileFormat.PUFFIN);
            assertThat(delete.referencedDataFile()).isNotNull();
            assertThat(delete.contentOffset()).isNotNull();
            assertThat(delete.contentSizeInBytes()).isPositive();
        }
        RowDelta delta = table.newRowDelta();
        committable.getDataFiles().forEach(delta::addRows);
        committable.getDeleteFiles().forEach(delta::addDeletes);
        delta.commit();

        Map<Integer, String> expected = new HashMap<>();
        for (int i = 0; i < 2000; i++) {
            expected.put(i, "initial");
        }
        expected.put(0, "updated");
        expected.put(1999, "updated");
        expected.put(500, "reinserted");
        assertRows(expected);
    }

    @Test
    void testEqualityDeletesAcrossCommitsAndDeleteThenReinsert() throws Exception {
        try (DeltaWriter writer = newWriter()) {
            writer.write(record(1, "old"));
            commit(writer.complete());
        }
        try (DeltaWriter writer = newWriter()) {
            writer.delete(record(1, "old"));
            writer.write(record(1, "new"));
            writer.delete(record(1, "new"));
            writer.write(record(1, "latest"));
            writer.write(record(2, "deleted"));
            writer.delete(record(2, "deleted"));
            WriteResult result = writer.complete();
            assertThat(result.deleteFiles())
                    .anyMatch(f -> f.content() == FileContent.EQUALITY_DELETES);
            assertThat(result.deleteFiles())
                    .filteredOn(f -> f.content() == FileContent.POSITION_DELETES)
                    .allMatch(ContentFileUtil::isDV);
            commit(result);
        }
        assertRows(Collections.singletonMap(1, "latest"));
        try (DeltaWriter writer = newWriter()) {
            writer.delete(record(1, "latest"));
            commit(writer.complete());
        }
        assertRows(Collections.emptyMap());
    }

    @Test
    void testEmptyWriter() throws IOException {
        try (TaskWriter<Record> writer = newWriter()) {
            WriteResult result = writer.complete();
            assertThat(result.dataFiles()).isEmpty();
            assertThat(result.deleteFiles()).isEmpty();
        }
    }

    @Test
    void testAbortDeletesOwnedFilesOnly() throws IOException {
        try (DeltaWriter previous = newWriter()) {
            previous.write(record(1, "committed"));
            commit(previous.complete());
        }
        DeltaWriter writer = newWriter();
        writer.write(record(2, "first"));
        writer.write(record(2, "last"));
        WriteResult result = writer.complete();
        writer.abort();
        Arrays.stream(result.dataFiles()).forEach(f -> assertMissing(table.io(), f.location()));
        Arrays.stream(result.deleteFiles()).forEach(f -> assertMissing(table.io(), f.location()));
        assertThatThrownBy(writer::complete).isInstanceOf(IllegalStateException.class);
        assertRows(Collections.singletonMap(1, "committed"));
    }

    @Test
    void testAbortAfterPuffinCloseFailure() throws IOException {
        FileIO delegate = table.io();
        List<String> outputs = new ArrayList<>();
        FileIO failingIO =
                new FileIO() {
                    @Override
                    public InputFile newInputFile(String path) {
                        return delegate.newInputFile(path);
                    }

                    @Override
                    public OutputFile newOutputFile(String path) {
                        outputs.add(path);
                        OutputFile output = delegate.newOutputFile(path);
                        return path.endsWith(".puffin") ? failingOutput(output) : output;
                    }

                    @Override
                    public void deleteFile(String path) {
                        delegate.deleteFile(path);
                    }
                };
        Table failingTable = spy(table);
        when(failingTable.io()).thenReturn(failingIO);
        DeltaWriter writer =
                (DeltaWriter) TaskWriterFactory.createTaskWriter(failingTable, null, 0);
        writer.write(record(1, "first"));
        writer.write(record(1, "second"));
        assertThatThrownBy(writer::complete)
                .isInstanceOf(IOException.class)
                .hasStackTraceContaining("injected Puffin close failure");
        assertThat(outputs).anyMatch(path -> path.endsWith(".puffin"));
        outputs.forEach(path -> assertMissing(delegate, path));
        assertThatThrownBy(writer::abort).isInstanceOf(IOException.class);
    }

    private static void assertMissing(FileIO io, String path) {
        assertThatThrownBy(() -> io.newInputFile(path)).isInstanceOf(NotFoundException.class);
    }

    private DeltaWriter newWriter() {
        return (DeltaWriter) TaskWriterFactory.createTaskWriter(table, null, 0);
    }

    private void commit(WriteResult result) {
        RowDelta delta = table.newRowDelta();
        Arrays.stream(result.dataFiles()).forEach(delta::addRows);
        Arrays.stream(result.deleteFiles()).forEach(delta::addDeletes);
        delta.commit();
    }

    private void assertRows(Map<Integer, String> expected) throws IOException {
        Map<Integer, String> actual = new HashMap<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record row : records) {
                assertThat(actual.put(row.get(0, Integer.class), row.get(1, String.class)))
                        .isNull();
            }
        }
        assertThat(actual).containsExactlyInAnyOrderEntriesOf(expected);
    }

    private static Record record(int id, String value) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("id", id);
        record.setField("value", value);
        return record;
    }

    private static OutputFile failingOutput(OutputFile delegate) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create() {
                return failingStream(delegate.create());
            }

            @Override
            public PositionOutputStream createOrOverwrite() {
                return failingStream(delegate.createOrOverwrite());
            }

            @Override
            public String location() {
                return delegate.location();
            }

            @Override
            public InputFile toInputFile() {
                return delegate.toInputFile();
            }
        };
    }

    private static PositionOutputStream failingStream(PositionOutputStream delegate) {
        return new PositionOutputStream() {
            @Override
            public long getPos() throws IOException {
                return delegate.getPos();
            }

            @Override
            public void write(int value) throws IOException {
                delegate.write(value);
            }

            @Override
            public void write(byte[] bytes, int offset, int length) throws IOException {
                delegate.write(bytes, offset, length);
            }

            @Override
            public void close() throws IOException {
                delegate.close();
                throw new IOException("injected Puffin close failure");
            }
        };
    }
}
