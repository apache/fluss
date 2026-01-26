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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for snapshot query. */
public class KvSnapshotQueryITCase extends ClientToServerITCaseBase {
    @BeforeEach
    protected void setup() throws Exception {
        super.setup();
    }

    @AfterEach
    protected void teardown() throws Exception {
        super.teardown();
    }

    @Test
    void testBasicScan() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_basic_scan");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();

        createTable(tablePath, descriptor, true);

        Table table = conn.getTable(tablePath);

        // 1. write data
        UpsertWriter writer = table.newUpsert().createWriter();
        writer.upsert(row(1, "a"));
        writer.upsert(row(2, "b"));
        writer.upsert(row(3, "c"));
        writer.flush();

        // 2. test the snapshotQuery works as expected
        List<InternalRow> result = snapshotQueryAll(table);

        assertThat(result).hasSize(3);
        result.sort(Comparator.comparingInt(r -> r.getInt(0)));
        assertThatRow(result.get(0)).withSchema(schema.getRowType()).isEqualTo(row(1, "a"));
        assertThatRow(result.get(1)).withSchema(schema.getRowType()).isEqualTo(row(2, "b"));
        assertThatRow(result.get(2)).withSchema(schema.getRowType()).isEqualTo(row(3, "c"));
    }

    @Test
    void testMultiBucketScan() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_multi_bucket_scan");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        // 3 buckets
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);
        Table table = conn.getTable(tablePath);

        // 1. write data to multiple buckets
        int rowCount = 100;
        UpsertWriter writer = table.newUpsert().createWriter();
        for (int i = 0; i < rowCount; i++) {
            writer.upsert(row(i, "val" + i));
        }
        writer.flush();

        // 2. scan all buckets and collect all data
        List<InternalRow> allResult = snapshotQueryAll(table);

        assertThat(allResult).hasSize(rowCount);
        allResult.sort(Comparator.comparingInt(r -> r.getInt(0)));
        for (int i = 0; i < rowCount; i++) {
            assertThatRow(allResult.get(i))
                    .withSchema(schema.getRowType())
                    .isEqualTo(row(i, "val" + i));
        }
    }

    @Test
    void testPartitionedTableScan() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_partitioned_scan");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("p", DataTypes.STRING())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id", "p")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("p")
                        .distributedBy(1, "id")
                        .build();

        createTable(tablePath, descriptor, true);
        admin.createPartition(
                        tablePath,
                        new PartitionSpec(java.util.Collections.singletonMap("p", "p1")),
                        false)
                .get();
        admin.createPartition(
                        tablePath,
                        new PartitionSpec(java.util.Collections.singletonMap("p", "p2")),
                        false)
                .get();

        Table table = conn.getTable(tablePath);

        UpsertWriter writer = table.newUpsert().createWriter();
        writer.upsert(row(1, "p1", "a1"));
        writer.upsert(row(2, "p1", "b1"));
        writer.upsert(row(1, "p2", "a2"));
        writer.flush();

        List<InternalRow> result = snapshotQueryAll(table);
        assertThat(result).hasSize(3);
    }

    @Test
    void testLargeDataScan() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_large_data_scan");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();

        createTable(tablePath, descriptor, true);
        Table table = conn.getTable(tablePath);

        // 1. write 10k records
        int rowCount = 10000;
        UpsertWriter writer = table.newUpsert().createWriter();
        for (int i = 0; i < rowCount; i++) {
            writer.upsert(row(i, "val" + i));
        }
        writer.flush();

        // 2. scan and verify
        List<InternalRow> result = snapshotQueryAll(table);

        assertThat(result).hasSize(rowCount);
        result.sort(Comparator.comparingInt(r -> r.getInt(0)));
        for (int i = 0; i < rowCount; i++) {
            assertThatRow(result.get(i))
                    .withSchema(schema.getRowType())
                    .isEqualTo(row(i, "val" + i));
        }
    }

    @Test
    void testSnapshotQuery() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_snapshot_query");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();

        createTable(tablePath, descriptor, true);
        Table table = conn.getTable(tablePath);

        // 1. write data
        UpsertWriter writer = table.newUpsert().createWriter();
        writer.upsert(row(1, "a"));
        writer.upsert(row(2, "b"));
        writer.upsert(row(3, "c"));
        writer.flush();

        // 2. test the snapshotQuery works as expected
        List<InternalRow> result = snapshotQueryAll(table);

        assertThat(result).hasSize(3);
        result.sort(Comparator.comparingInt(r -> r.getInt(0)));
        assertThatRow(result.get(0)).withSchema(schema.getRowType()).isEqualTo(row(1, "a"));
        assertThatRow(result.get(1)).withSchema(schema.getRowType()).isEqualTo(row(2, "b"));
        assertThatRow(result.get(2)).withSchema(schema.getRowType()).isEqualTo(row(3, "c"));
    }

    private List<InternalRow> snapshotQueryAll(Table table) throws Exception {
        List<InternalRow> allRows = new ArrayList<>();
        try (CloseableIterator<InternalRow> iterator = table.newSnapshotQuery().execute()) {
            while (iterator.hasNext()) {
                allRows.add(iterator.next());
            }
        }
        return allRows;
    }
}
