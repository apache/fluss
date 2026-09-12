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

import org.apache.fluss.client.write.WriteRecord;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Regression test for {@link TypedUpsertWriterImpl#delete}: the logical primary key of a
 * partitioned table includes the partition column, but the physical primary key excludes it. Using
 * the physical primary key to build the delete row silently drops the partition column, so {@link
 * AbstractTableWriter#getPhysicalPath} fails to resolve the partition.
 */
class TypedUpsertWriterImplPartitionKeyTest {

    private static final TablePath PARTITIONED_TABLE_PATH =
            TablePath.of("test_db", "partitioned_pk_table");
    private static final TablePath NON_PARTITIONED_TABLE_PATH = TablePath.of("test_db", "pk_table");

    /** POJO matching the {@code id, dt, payload} table schema used by both tests. */
    public static class RecordPojo {
        public Integer id;
        public String dt;
        public String payload;

        public RecordPojo() {}

        public RecordPojo(Integer id, String dt, String payload) {
            this.id = id;
            this.dt = dt;
            this.payload = payload;
        }
    }

    @Test
    void testTypedDeleteOnPartitionedTableResolvesPartition() {
        TableInfo tableInfo = createTableInfo(PARTITIONED_TABLE_PATH, true);
        WriterClient writerClient = mock(WriterClient.class);
        TypedUpsertWriter<RecordPojo> writer =
                new TableUpsert(PARTITIONED_TABLE_PATH, tableInfo, writerClient)
                        .createTypedWriter(RecordPojo.class);

        RecordPojo pojo = new RecordPojo(42, "2026-09-11", null);
        writer.delete(pojo);

        ArgumentCaptor<WriteRecord> captor = ArgumentCaptor.forClass(WriteRecord.class);
        verify(writerClient).send(captor.capture(), ArgumentMatchers.any());
        assertThat(captor.getValue().getPhysicalTablePath())
                .isEqualTo(PhysicalTablePath.of(PARTITIONED_TABLE_PATH, "2026-09-11"));
    }

    @Test
    void testTypedDeleteOnNonPartitionedTableResolvesPath() {
        TableInfo tableInfo = createTableInfo(NON_PARTITIONED_TABLE_PATH, false);
        WriterClient writerClient = mock(WriterClient.class);
        TypedUpsertWriter<RecordPojo> writer =
                new TableUpsert(NON_PARTITIONED_TABLE_PATH, tableInfo, writerClient)
                        .createTypedWriter(RecordPojo.class);

        RecordPojo pojo = new RecordPojo(42, "2026-09-11", null);
        writer.delete(pojo);

        ArgumentCaptor<WriteRecord> captor = ArgumentCaptor.forClass(WriteRecord.class);
        verify(writerClient).send(captor.capture(), ArgumentMatchers.any());
        assertThat(captor.getValue().getPhysicalTablePath())
                .isEqualTo(PhysicalTablePath.of(NON_PARTITIONED_TABLE_PATH));
    }

    private static TableInfo createTableInfo(TablePath tablePath, boolean partitioned) {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .column("payload", DataTypes.STRING());
        Schema schema =
                partitioned
                        ? schemaBuilder.primaryKey("id", "dt").build()
                        : schemaBuilder.primaryKey("id").build();
        TableDescriptor.Builder descriptorBuilder =
                TableDescriptor.builder().schema(schema).distributedBy(1);
        if (partitioned) {
            descriptorBuilder.partitionedBy("dt");
        }
        return TableInfo.of(tablePath, 1L, 1, descriptorBuilder.build(), null, 0L, 0L);
    }
}
