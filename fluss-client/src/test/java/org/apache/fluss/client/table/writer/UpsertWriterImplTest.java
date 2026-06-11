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

import org.apache.fluss.client.write.WriteCallback;
import org.apache.fluss.client.write.WriteRecord;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionKey;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/** Tests for {@link UpsertWriterImpl}. */
class UpsertWriterImplTest {

    @Test
    void testDeleteAcceptsPrimaryKeyRowForImplicitPartitionedTable() throws Exception {
        TableInfo tableInfo = implicitPartitionedTableInfo();
        AtomicReference<WriteRecord> sentRecord = new AtomicReference<>();
        WriterClient writerClient = mock(WriterClient.class);
        doAnswer(
                        invocation -> {
                            WriteRecord record = invocation.getArgument(0);
                            WriteCallback callback = invocation.getArgument(1);
                            sentRecord.set(record);
                            callback.onCompletion(null, 11L, null);
                            return null;
                        })
                .when(writerClient)
                .send(any(WriteRecord.class), any(WriteCallback.class));
        UpsertWriterImpl writer =
                new UpsertWriterImpl(tableInfo.getTablePath(), tableInfo, null, writerClient);
        LocalDateTime eventTime = LocalDateTime.of(2024, 3, 15, 10, 30);

        DeleteResult result =
                writer.delete(
                                GenericRow.of(
                                        BinaryString.fromString("us"),
                                        7,
                                        TimestampNtz.fromLocalDateTime(eventTime)))
                        .get();

        assertThat(result.getLogEndOffset()).isEqualTo(11L);
        assertThat(sentRecord.get().getPhysicalTablePath().getPartitionName())
                .isEqualTo("us$20240315");
        assertThat(sentRecord.get().getRow()).isNull();
        assertThat(sentRecord.get().getKey()).isNotNull();
        assertThat(sentRecord.get().getBucketKey()).isNotNull();
    }

    @Test
    void testTypedDeleteDelegatesLogicalPrimaryKeyRow() throws Exception {
        TableInfo tableInfo = implicitPartitionedTableInfo();
        CapturingUpsertWriter delegate = new CapturingUpsertWriter();
        TypedUpsertWriterImpl<EventPojo> writer =
                new TypedUpsertWriterImpl<>(delegate, EventPojo.class, tableInfo, null);
        EventPojo event = new EventPojo();
        event.region = "us";
        event.id = 7;
        event.eventTime = LocalDateTime.of(2024, 3, 15, 10, 30);
        event.payload = "payload";

        writer.delete(event).get();

        InternalRow row = delegate.deletedRow.get();
        assertThat(row.getFieldCount()).isEqualTo(tableInfo.getPrimaryKeys().size());
        assertThat(row.getString(0).toString()).isEqualTo("us");
        assertThat(row.getInt(1)).isEqualTo(7);
        assertThat(row.getTimestampNtz(2, 6))
                .isEqualTo(TimestampNtz.fromLocalDateTime(event.eventTime));
    }

    @Test
    void testDeleteUsesFullRowWhenPrimaryKeyRowCountMatchesTableRowCount() throws Exception {
        TableInfo tableInfo = allColumnsPrimaryKeyTableInfo();
        AtomicReference<WriteRecord> sentRecord = new AtomicReference<>();
        WriterClient writerClient = mock(WriterClient.class);
        doAnswer(
                        invocation -> {
                            WriteRecord record = invocation.getArgument(0);
                            WriteCallback callback = invocation.getArgument(1);
                            sentRecord.set(record);
                            callback.onCompletion(null, 11L, null);
                            return null;
                        })
                .when(writerClient)
                .send(any(WriteRecord.class), any(WriteCallback.class));
        UpsertWriterImpl writer =
                new UpsertWriterImpl(tableInfo.getTablePath(), tableInfo, null, writerClient);

        DeleteResult result = writer.delete(GenericRow.of(7, BinaryString.fromString("us"))).get();

        assertThat(result.getLogEndOffset()).isEqualTo(11L);
        assertThat(sentRecord.get().getPhysicalTablePath().getPartitionName()).isNull();
        assertThat(sentRecord.get().getRow()).isNull();
        assertThat(sentRecord.get().getKey()).isNotNull();
        assertThat(sentRecord.get().getBucketKey()).isNotNull();
    }

    @Test
    void testTypedDeleteUsesFullRowWhenPrimaryKeyRowCountMatchesTableRowCount() throws Exception {
        TableInfo tableInfo = allColumnsPrimaryKeyTableInfo();
        CapturingUpsertWriter delegate = new CapturingUpsertWriter();
        TypedUpsertWriterImpl<AllPrimaryKeyPojo> writer =
                new TypedUpsertWriterImpl<>(delegate, AllPrimaryKeyPojo.class, tableInfo, null);
        AllPrimaryKeyPojo record = new AllPrimaryKeyPojo();
        record.id = 7;
        record.region = "us";

        writer.delete(record).get();

        InternalRow row = delegate.deletedRow.get();
        assertThat(row.getFieldCount()).isEqualTo(tableInfo.getRowType().getFieldCount());
        assertThat(row.getInt(0)).isEqualTo(7);
        assertThat(row.getString(1).toString()).isEqualTo("us");
    }

    private static TableInfo implicitPartitionedTableInfo() {
        Schema schema =
                Schema.newBuilder()
                        .column("region", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .column("eventTime", DataTypes.TIMESTAMP())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("region", "id", "eventTime")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedByKeys(
                                PartitionKey.column("region"),
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "eventDay",
                                                DateTruncPartitionTransform.of(
                                                        "eventTime", AutoPartitionTimeUnit.DAY))))
                        .distributedBy(1)
                        .build();
        return TableInfo.of(TablePath.of("db", "t"), 1, 0, descriptor, null, 0, 0);
    }

    private static TableInfo allColumnsPrimaryKeyTableInfo() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("region", DataTypes.STRING())
                        .primaryKey("region", "id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        return TableInfo.of(TablePath.of("db", "t2"), 2, 0, descriptor, null, 0, 0);
    }

    /** POJO matching the full table schema. */
    public static class EventPojo {
        public String region;
        public Integer id;
        public LocalDateTime eventTime;
        public String payload;

        public EventPojo() {}
    }

    /** POJO for a table whose primary key contains every physical column. */
    public static class AllPrimaryKeyPojo {
        public Integer id;
        public String region;

        public AllPrimaryKeyPojo() {}
    }

    private static class CapturingUpsertWriter implements UpsertWriter {
        private final AtomicReference<InternalRow> deletedRow = new AtomicReference<>();

        @Override
        public void flush() {}

        @Override
        public CompletableFuture<UpsertResult> upsert(InternalRow record) {
            return CompletableFuture.completedFuture(UpsertResult.empty());
        }

        @Override
        public CompletableFuture<DeleteResult> delete(InternalRow record) {
            deletedRow.set(record);
            return CompletableFuture.completedFuture(DeleteResult.empty());
        }
    }
}
