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

package org.apache.fluss.client.table;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.TypedAppendWriter;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.testutils.InternalRowListAssert;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.client.table.scanner.batch.BatchScanUtils.collectRows;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Deduplication scope and contention contracts through independent public writers. */
class DeduplicationScopeITCase extends ClientToServerITCaseBase {

    @Test
    void testPartitionInstancesHaveIndependentKeys() throws Exception {
        TablePath path = TablePath.of("dedup_scope", "partitioned");
        Schema schema =
                Schema.newBuilder()
                        .column("dt", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .deduplicationKey("dt", "id")
                        .build();
        createTable(
                path,
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("dt")
                        .distributedBy(1)
                        .build(),
                false);
        admin.createPartition(path, newPartitionSpec("dt", "a"), false).get();
        admin.createPartition(path, newPartitionSpec("dt", "b"), false).get();
        try (Table table = conn.getTable(path)) {
            AppendWriter writer = table.newAppend().createWriter();
            append(writer, row("a", 1));
            append(writer, row("b", 1));
            append(writer, row("a", 1));
            for (PartitionInfo partition : admin.listPartitionInfos(path).get()) {
                assertThat(
                                collectRows(
                                        table.newScan()
                                                .limit(100)
                                                .createBatchScanner(
                                                        new TableBucket(
                                                                table.getTableInfo().getTableId(),
                                                                partition.getPartitionId(),
                                                                0))))
                        .hasSize(1);
            }
            assertThatThrownBy(() -> table.newUpsert()).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> table.newLookup()).isInstanceOf(IllegalStateException.class);
        }
        long previous =
                admin.listPartitionInfos(path).get().stream()
                        .filter(p -> p.getPartitionName().equals("a"))
                        .findFirst()
                        .get()
                        .getPartitionId();
        admin.dropPartition(path, newPartitionSpec("dt", "a"), false).get();
        admin.createPartition(path, newPartitionSpec("dt", "a"), false).get();
        assertThat(
                        admin.listPartitionInfos(path).get().stream()
                                .filter(p -> p.getPartitionName().equals("a"))
                                .findFirst()
                                .get()
                                .getPartitionId())
                .isNotEqualTo(previous);
        try (Connection fresh = ConnectionFactory.createConnection(clientConf);
                Table table = fresh.getTable(path)) {
            append(table.newAppend().createWriter(), row("a", 1));
            long partitionId =
                    admin.listPartitionInfos(path).get().stream()
                            .filter(p -> p.getPartitionName().equals("a"))
                            .findFirst()
                            .get()
                            .getPartitionId();
            assertThat(
                            collectRows(
                                    table.newScan()
                                            .limit(100)
                                            .createBatchScanner(
                                                    new TableBucket(
                                                            table.getTableInfo().getTableId(),
                                                            partitionId,
                                                            0))))
                    .hasSize(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testBucketKeyCanEqualOrBeSubsetOfDeduplicationKey(boolean subset) throws Exception {
        TablePath path = TablePath.of("dedup_scope", "bucket_keys_" + subset);
        Schema schema =
                Schema.newBuilder()
                        .column("tenant", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .deduplicationKey("tenant", "id")
                        .build();
        createTable(
                path,
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(
                                4, subset ? new String[] {"tenant"} : new String[] {"tenant", "id"})
                        .build(),
                false);
        try (Table table = conn.getTable(path)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (String tenant : Arrays.asList("a", "b")) {
                for (int id = 0; id < 2; id++) {
                    writer.append(row(tenant, id, "first"));
                    writer.append(row(tenant, id, "replacement"));
                }
            }
            writer.flush();
            List<InternalRow> rows = new ArrayList<>();
            for (int bucket = 0; bucket < 4; bucket++) {
                rows.addAll(
                        collectRows(
                                table.newScan()
                                        .limit(100)
                                        .createBatchScanner(
                                                new TableBucket(
                                                        table.getTableInfo().getTableId(),
                                                        null,
                                                        bucket))));
            }
            rows.sort(
                    (left, right) -> {
                        int tenant =
                                left.getString(0)
                                        .toString()
                                        .compareTo(right.getString(0).toString());
                        return tenant == 0
                                ? Integer.compare(left.getInt(1), right.getInt(1))
                                : tenant;
                    });
            InternalRowListAssert.assertThatRows(rows)
                    .withSchema(schema.getRowType())
                    .isEqualTo(
                            Arrays.asList(
                                    row("a", 0, "first"),
                                    row("a", 1, "first"),
                                    row("b", 0, "first"),
                                    row("b", 1, "first")));
        }
    }

    @Test
    void testIndependentWritersChooseOneWinner() throws Exception {
        TablePath path = TablePath.of("dedup_scope", "concurrent");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .deduplicationKey("id")
                        .build();
        createTable(path, TableDescriptor.builder().schema(schema).distributedBy(1).build(), false);
        try (Connection other = ConnectionFactory.createConnection(clientConf);
                Table first = conn.getTable(path);
                Table second = other.getTable(path)) {
            AppendWriter a = first.newAppend().createWriter();
            TypedAppendWriter<Event> b = second.newAppend().createTypedWriter(Event.class);
            CompletableFuture<AppendResult> ar = a.append(row(1, "a"));
            CompletableFuture<AppendResult> br = b.append(new Event(1, "b"));
            a.flush();
            b.flush();
            ar.get(30, TimeUnit.SECONDS);
            br.get(30, TimeUnit.SECONDS);
            List<InternalRow> rows =
                    collectRows(
                            first.newScan()
                                    .limit(100)
                                    .createBatchScanner(
                                            new TableBucket(
                                                    first.getTableInfo().getTableId(), null, 0)));
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getInt(0)).isEqualTo(1);
            assertThat(rows.get(0).getString(1).toString()).isIn("a", "b");
        }
    }

    private static void append(AppendWriter writer, GenericRow row) throws Exception {
        CompletableFuture<AppendResult> result = writer.append(row);
        writer.flush();
        result.get(30, TimeUnit.SECONDS);
    }

    /** Event used to verify typed append filtering. */
    public static class Event {
        public Integer id;
        public String payload;

        public Event() {}

        public Event(int id, String payload) {
            this.id = id;
            this.payload = payload;
        }
    }
}
