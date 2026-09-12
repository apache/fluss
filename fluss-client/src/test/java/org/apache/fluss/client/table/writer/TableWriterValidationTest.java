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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.write.WriteCallback;
import org.apache.fluss.client.write.WriteRecord;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.indexedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests writer-side NOT NULL validation for append, upsert, and delete entry points. */
class TableWriterValidationTest {
    private static final TablePath TABLE_PATH = TablePath.of("db", "sink");
    private static final RowType SOURCE_TYPE =
            RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.INT());
    private static final RowType DELETE_SOURCE_TYPE =
            RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING());

    @ParameterizedTest
    @MethodSource("invalidAppendCases")
    void testAppendRejectsNotNullBeforeSending(
            LogFormat format, RowRepresentation representation, boolean multi, int nullIndex) {
        TableInfo info = logTableInfo(format, false);
        WriterClient client = writerClient();
        Object[] values = {1, "value", 7};
        values[nullIndex] = null;
        InternalRow input = row(representation, values);
        String column = info.getRowType().getFieldNames().get(nullIndex);
        assertThat(input.isNullAt(nullIndex)).isTrue();

        if (multi) {
            CompletableFuture<WriteResult> result =
                    multiWriter(info, client)
                            .write(
                                    MultiTableWriteRecord.forAppend(
                                            TABLE_PATH, input, info.getSchemaId()));
            assertThat(result).isCompletedExceptionally();
            assertThatThrownBy(result::join)
                    .hasCauseInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'" + column + "'")
                    .hasMessageContaining("NOT NULL");
        } else {
            assertThatThrownBy(() -> new AppendWriterImpl(TABLE_PATH, info, client).append(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'" + column + "'")
                    .hasMessageContaining("NOT NULL");
        }

        verify(client, never()).send(any(), any());
    }

    @ParameterizedTest
    @MethodSource("appendCases")
    void testAppendPreservesNullableNullsForFastPathAndReencoding(
            LogFormat format, RowRepresentation representation, boolean multi) {
        for (boolean nullable : new boolean[] {false, true}) {
            TableInfo info = logTableInfo(format, nullable);
            AtomicReference<WriteRecord> sent = new AtomicReference<>();
            WriterClient client = writerClient(sent);
            InternalRow input = row(representation, new Object[] {1, nullable ? null : "value", 7});

            CompletableFuture<?> result;
            if (multi) {
                result =
                        multiWriter(info, client)
                                .write(
                                        MultiTableWriteRecord.forAppend(
                                                TABLE_PATH, input, info.getSchemaId()));
            } else {
                result = new AppendWriterImpl(TABLE_PATH, info, client).append(input);
            }

            assertThat(result).isCompleted();
            verify(client).send(any(), any());
            InternalRow encoded = sent.get().getRow();
            assertThat(encoded.isNullAt(1)).isEqualTo(nullable);
            if (format == LogFormat.ARROW || representation.matches(format)) {
                assertThat(encoded).isSameAs(input);
            } else {
                assertThat(encoded).isNotSameAs(input);
            }
        }
    }

    private static List<Arguments> appendCases() {
        List<Arguments> cases = new ArrayList<>();
        for (LogFormat format : LogFormat.values()) {
            for (RowRepresentation representation : RowRepresentation.values()) {
                for (boolean multi : new boolean[] {false, true}) {
                    cases.add(Arguments.of(format, representation, multi));
                }
            }
        }
        return cases;
    }

    private static List<Arguments> invalidAppendCases() {
        List<Arguments> cases = new ArrayList<>();
        for (LogFormat format : LogFormat.values()) {
            for (RowRepresentation representation : RowRepresentation.values()) {
                for (boolean multi : new boolean[] {false, true}) {
                    for (int nullIndex : new int[] {1, 2}) {
                        cases.add(Arguments.of(format, representation, multi, nullIndex));
                    }
                }
            }
        }
        return cases;
    }

    @ParameterizedTest
    @MethodSource("invalidBinaryRows")
    void testRejectsBinaryNullBeforeSending(
            KvFormat format, boolean multi, int nullIndex, String representation) {
        Object[] values = {1, "value", 7};
        values[nullIndex] = null;
        InternalRow input;
        if (representation.equals("aligned")) {
            input = alignedRow(nullIndex);
        } else if (representation.equals("indexed")) {
            input = indexedRow(SOURCE_TYPE, values);
        } else {
            input = compactedRow(SOURCE_TYPE, values);
        }
        assertThat(input.isNullAt(nullIndex)).isTrue();
        assertRejected(tableInfo(format, false), input, nullIndex, multi);
    }

    private static List<Arguments> invalidBinaryRows() {
        List<Arguments> cases = new ArrayList<>();
        for (KvFormat format : KvFormat.values()) {
            for (boolean multi : new boolean[] {false, true}) {
                for (int nullIndex : new int[] {1, 2, 0}) {
                    for (String representation : new String[] {"aligned", "indexed", "compacted"}) {
                        cases.add(Arguments.of(format, multi, nullIndex, representation));
                    }
                }
            }
        }
        return cases;
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testRejectsProjectedNull(KvFormat format) {
        TableInfo info = tableInfo(format, false);
        // The logical id is at physical index 2; count reads the null at physical index 0.
        InternalRow reordered =
                ProjectedRow.from(new int[] {2, 1, 0})
                        .replaceRow(indexedRow(SOURCE_TYPE, new Object[] {null, "value", 1}));
        assertRejected(info, reordered, 2, false);
        assertRejected(info, reordered, 2, true);
        InternalRow missing =
                ProjectedRow.from(new int[] {0, -1, 2})
                        .replaceRow(indexedRow(SOURCE_TYPE, new Object[] {1, "value", 7}));
        assertRejected(info, missing, 1, false);
        assertRejected(info, missing, 1, true);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testAcceptsValidValuesAndNullableNulls(KvFormat format) {
        for (boolean nullable : new boolean[] {false, true}) {
            TableInfo info = tableInfo(format, nullable);
            Object[] values = {1, nullable ? null : "value", nullable ? null : 7};
            InternalRow input =
                    format == KvFormat.INDEXED
                            ? indexedRow(SOURCE_TYPE, values)
                            : compactedRow(SOURCE_TYPE, values);
            WriterClient directClient = writerClient();
            assertThat(new UpsertWriterImpl(TABLE_PATH, info, null, directClient).upsert(input))
                    .isCompleted();
            verify(directClient).send(any(), any());
            WriterClient multiClient = writerClient();
            assertThat(
                            multiWriter(info, multiClient)
                                    .write(
                                            MultiTableWriteRecord.forUpsert(
                                                    TABLE_PATH, input, info.getSchemaId())))
                    .isCompleted();
            verify(multiClient).send(any(), any());
        }
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testPartialUpdateChecksSelectedPositions(KvFormat format) {
        Schema schema =
                Schema.newBuilder()
                        .column("sequence", DataTypes.BIGINT().copy(false))
                        .column("payload", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .primaryKey("id")
                        .enableAutoIncrement("sequence")
                        .build();
        TableInfo info = tableInfo(format, schema);
        WriterClient client = writerClient();
        UpsertWriterImpl writer = new UpsertWriterImpl(TABLE_PATH, info, new int[] {2, 1}, client);
        // Input remains full-width; the omitted auto-increment column is a null placeholder.
        RowType sourceType = RowType.of(DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.INT());
        InternalRow input =
                format == KvFormat.INDEXED
                        ? indexedRow(sourceType, new Object[] {null, null, 1})
                        : compactedRow(sourceType, new Object[] {null, null, 1});
        assertThat(info.getRowType().getTypeAt(0).isNullable()).isFalse();
        assertThat(input.isNullAt(0)).isTrue();
        assertThat(writer.upsert(input)).isCompleted();
        verify(client).send(any(), any());
        assertThatThrownBy(() -> writer.upsert(GenericRow.of(null, null, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'id'");
        verify(client).send(any(), any());
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testDeleteAllowsNullNonKeyPlaceholders(KvFormat format) {
        TableInfo info = tableInfo(format, false);
        InternalRow input = GenericRow.of(1, null, null);
        WriterClient directClient = writerClient();
        assertThat(new UpsertWriterImpl(TABLE_PATH, info, null, directClient).delete(input))
                .isCompleted();
        verify(directClient).send(any(), any());
        WriterClient multiClient = writerClient();
        assertThat(
                        multiWriter(info, multiClient)
                                .write(
                                        MultiTableWriteRecord.forDelete(
                                                TABLE_PATH, input, info.getSchemaId())))
                .isCompleted();
        verify(multiClient).send(any(), any());
    }

    @ParameterizedTest
    @MethodSource("deleteCases")
    void testDeleteRejectsNullPhysicalPrimaryKeyAndPartitionKey(
            KvFormat format, boolean multi, int nullIndex, String column) {
        TableInfo info = partitionedTableInfo(format);
        Object[] values = {1, "p", null};
        values[nullIndex] = null;
        InternalRow input = indexedRow(DELETE_SOURCE_TYPE, values);
        assertThat(input.isNullAt(nullIndex)).isTrue();
        WriterClient client = writerClient();

        if (multi) {
            CompletableFuture<WriteResult> result =
                    multiWriter(info, client)
                            .write(
                                    MultiTableWriteRecord.forDelete(
                                            TABLE_PATH, input, info.getSchemaId()));
            assertThat(result).isCompletedExceptionally();
            assertThatThrownBy(result::join)
                    .hasCauseInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'" + column + "'")
                    .hasMessageContaining("NOT NULL");
        } else {
            assertThatThrownBy(
                            () ->
                                    new UpsertWriterImpl(TABLE_PATH, info, null, client)
                                            .delete(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'" + column + "'")
                    .hasMessageContaining("NOT NULL");
        }

        verify(client, never()).send(any(), any());
    }

    private static List<Arguments> deleteCases() {
        List<Arguments> cases = new ArrayList<>();
        for (KvFormat format : KvFormat.values()) {
            for (boolean multi : new boolean[] {false, true}) {
                cases.add(Arguments.of(format, multi, 0, "id"));
                cases.add(Arguments.of(format, multi, 1, "partition"));
            }
        }
        return cases;
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testPartialDeleteAllowsOmittedNullableColumns(KvFormat format) {
        TableInfo info = tableInfo(format, true);
        WriterClient client = writerClient();
        UpsertWriterImpl writer = new UpsertWriterImpl(TABLE_PATH, info, new int[] {0}, client);

        assertThat(writer.delete(GenericRow.of(1, null, null))).isCompleted();
        verify(client).send(any(), any());
    }

    @Test
    void testTypedAppendAndUpsertDelegateValidRows() {
        WriterPojo pojo = new WriterPojo(1, null, 7);
        AtomicReference<WriteRecord> appendRecord = new AtomicReference<>();
        WriterClient appendClient = writerClient(appendRecord);
        TableInfo appendInfo = logTableInfo(LogFormat.ARROW, true);
        new TypedAppendWriterImpl<>(
                        new AppendWriterImpl(TABLE_PATH, appendInfo, appendClient),
                        WriterPojo.class,
                        appendInfo)
                .append(pojo);
        assertThat(appendRecord.get().getRow().isNullAt(1)).isTrue();

        AtomicReference<WriteRecord> upsertRecord = new AtomicReference<>();
        WriterClient upsertClient = writerClient(upsertRecord);
        TableInfo upsertInfo = tableInfo(KvFormat.INDEXED, true);
        new TypedUpsertWriterImpl<>(
                        new UpsertWriterImpl(TABLE_PATH, upsertInfo, null, upsertClient),
                        WriterPojo.class,
                        upsertInfo,
                        null)
                .upsert(pojo);
        assertThat(upsertRecord.get().getRow().isNullAt(1)).isTrue();
    }

    @Test
    void testMultiTableFirstHistoricalSchemaUsesHistoricalNotNullConstraints() {
        Schema historical =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING().copy(false))
                        .build();
        Schema latest =
                Schema.newBuilder()
                        .fromSchema(historical)
                        .column("extra", DataTypes.STRING())
                        .build();
        TableInfo latestInfo = logTableInfo(LogFormat.ARROW, latest, 2);
        Admin admin = mock(Admin.class);
        when(admin.getTableInfo(TABLE_PATH))
                .thenReturn(CompletableFuture.completedFuture(latestInfo));
        when(admin.getTableSchema(TABLE_PATH, 1))
                .thenReturn(CompletableFuture.completedFuture(new SchemaInfo(historical, 1)));

        WriterClient validClient = writerClient();
        MultiTableWriterImpl validWriter =
                new MultiTableWriterImpl(mock(MetadataUpdater.class), admin, validClient);
        assertThat(
                        validWriter.write(
                                MultiTableWriteRecord.forAppend(
                                        TABLE_PATH, GenericRow.of(1, "old"), 1)))
                .isCompleted();
        verify(validClient).send(any(), any());

        WriterClient rejectedClient = writerClient();
        MultiTableWriterImpl rejectedWriter =
                new MultiTableWriterImpl(mock(MetadataUpdater.class), admin, rejectedClient);
        CompletableFuture<WriteResult> rejected =
                rejectedWriter.write(
                        MultiTableWriteRecord.forAppend(TABLE_PATH, GenericRow.of(1, null), 1));
        assertThat(rejected).isCompletedExceptionally();
        assertThatThrownBy(rejected::join)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'payload'")
                .hasMessageContaining("NOT NULL");
        verify(rejectedClient, never()).send(any(), any());
    }

    private static void assertRejected(
            TableInfo info, InternalRow input, int nullIndex, boolean multi) {
        String column = info.getRowType().getFieldNames().get(nullIndex);
        WriterClient client = writerClient();
        if (multi) {
            CompletableFuture<WriteResult> result =
                    multiWriter(info, client)
                            .write(
                                    MultiTableWriteRecord.forUpsert(
                                            TABLE_PATH, input, info.getSchemaId()));
            assertThat(result).isCompletedExceptionally();
            assertThatThrownBy(result::join)
                    .hasCauseInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'" + column + "'")
                    .hasMessageContaining("NOT NULL");
        } else {
            UpsertWriterImpl direct = new UpsertWriterImpl(TABLE_PATH, info, null, client);
            assertThatThrownBy(() -> direct.upsert(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'" + column + "'")
                    .hasMessageContaining("NOT NULL")
                    .hasMessageContaining(TABLE_PATH.toString());
        }
        verify(client, never()).send(any(), any());
    }

    private static InternalRow row(RowRepresentation representation, Object[] values) {
        if (representation == RowRepresentation.INDEXED) {
            return indexedRow(SOURCE_TYPE, values);
        }
        if (representation == RowRepresentation.COMPACTED) {
            return compactedRow(SOURCE_TYPE, values);
        }
        AlignedRow input = new AlignedRow(3);
        AlignedRowWriter writer = new AlignedRowWriter(input);
        writer.writeInt(0, (int) values[0]);
        if (values[1] == null) {
            writer.setNullAt(1);
        } else {
            writer.writeString(1, BinaryString.fromString((String) values[1]));
        }
        if (values[2] == null) {
            writer.setNullAt(2);
        } else {
            writer.writeInt(2, (int) values[2]);
        }
        writer.complete();
        return input;
    }

    private static AlignedRow alignedRow(int nullIndex) {
        AlignedRow input = new AlignedRow(3);
        AlignedRowWriter writer = new AlignedRowWriter(input);
        writer.writeInt(0, 1);
        writer.writeString(1, BinaryString.fromString("value"));
        writer.writeInt(2, 7);
        writer.setNullAt(nullIndex);
        writer.complete();
        return input;
    }

    private static TableInfo tableInfo(KvFormat format, boolean nullable) {
        return tableInfo(
                format,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING().copy(nullable))
                        .column("count", DataTypes.INT().copy(nullable))
                        .primaryKey("id")
                        .build());
    }

    private static TableInfo logTableInfo(LogFormat format, boolean nullable) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING().copy(nullable))
                        .column("count", DataTypes.INT().copy(nullable))
                        .build();
        return logTableInfo(format, schema, 1);
    }

    private static TableInfo logTableInfo(LogFormat format, Schema schema, int schemaId) {
        return TableInfo.of(
                TABLE_PATH,
                1,
                schemaId,
                TableDescriptor.builder().schema(schema).distributedBy(1).logFormat(format).build(),
                "test",
                0,
                0);
    }

    private static TableInfo partitionedTableInfo(KvFormat format) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("partition", DataTypes.STRING().copy(false))
                        .column("payload", DataTypes.STRING().copy(false))
                        .primaryKey("id", "partition")
                        .build();
        return TableInfo.of(
                TABLE_PATH,
                1,
                1,
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("partition")
                        .distributedBy(1)
                        .kvFormat(format)
                        .build(),
                "test",
                0,
                0);
    }

    private static TableInfo tableInfo(KvFormat format, Schema schema) {
        return TableInfo.of(
                TABLE_PATH,
                1,
                1,
                TableDescriptor.builder().schema(schema).distributedBy(1).kvFormat(format).build(),
                "test",
                0,
                0);
    }

    private static MultiTableWriterImpl multiWriter(TableInfo info, WriterClient client) {
        Admin admin = mock(Admin.class);
        when(admin.getTableInfo(TABLE_PATH)).thenReturn(CompletableFuture.completedFuture(info));
        return new MultiTableWriterImpl(mock(MetadataUpdater.class), admin, client);
    }

    private static WriterClient writerClient() {
        return writerClient(null);
    }

    private static WriterClient writerClient(AtomicReference<WriteRecord> sent) {
        WriterClient client = mock(WriterClient.class);
        doAnswer(
                        invocation -> {
                            if (sent != null) {
                                sent.set(invocation.getArgument(0));
                            }
                            WriteCallback callback = invocation.getArgument(1);
                            callback.onCompletion(null, 1, null);
                            return null;
                        })
                .when(client)
                .send(any(), any());
        return client;
    }

    private enum RowRepresentation {
        ALIGNED,
        INDEXED,
        COMPACTED;

        boolean matches(LogFormat format) {
            return (this == INDEXED && format == LogFormat.INDEXED)
                    || (this == COMPACTED && format == LogFormat.COMPACTED);
        }
    }

    /** POJO used to verify typed writers delegate valid rows. */
    public static class WriterPojo {
        public Integer id;
        public String payload;
        public Integer count;

        public WriterPojo() {}

        WriterPojo(Integer id, String payload, Integer count) {
            this.id = id;
            this.payload = payload;
            this.count = count;
        }
    }
}
