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

package org.apache.fluss.client.bulkload.protocol;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanUtils;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.AbortBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Bulk-loaded snapshots through ordinary client reads, writes and scans. */
final class BulkLoadServerITCase {
    private static final Duration TIMEOUT = Duration.ofMinutes(3);
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("payload", DataTypes.STRING())
                    .primaryKey("id")
                    .build();
    private static final Schema PARTITIONED =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("payload", DataTypes.STRING())
                    .column("pt", DataTypes.STRING())
                    .primaryKey("id", "pt")
                    .build();
    private final FlussClusterExtension cluster =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1)))
                    .build();
    private Connection connection;
    private FlussAdmin admin;

    @BeforeEach
    void start() throws Exception {
        cluster.start();
        connection = ConnectionFactory.createConnection(cluster.getClientConfig());
        admin = (FlussAdmin) connection.getAdmin();
    }

    @AfterEach
    void close() throws Exception {
        IOUtils.closeAll(admin, connection, cluster::close);
    }

    @Test
    void testPartitionsKeepIndependentFrozenBucketLayouts() throws Exception {
        TablePath path = create("partition_layouts", true);
        await(
                admin.alterTable(
                        path, Collections.singletonList(TableChange.modifyBucketCount(3)), false));
        await(
                admin.createPartition(
                        path, new PartitionSpec(Collections.singletonMap("pt", "p4")), false));
        PhysicalTablePath first = PhysicalTablePath.of(path, "p1");
        PhysicalTablePath second = PhysicalTablePath.of(path, "p4");
        BeginBulkLoadResponse begun = begin(first, second);
        assertThat(begun.getTargetAt(0).getBucketCount()).isEqualTo(2);
        assertThat(begun.getTargetAt(1).getBucketCount()).isEqualTo(3);
        Map<PhysicalTablePath, List<Object[]>> rows = new HashMap<>();
        rows.put(first, Collections.singletonList(new Object[] {1, "first", "p1"}));
        rows.put(second, Collections.singletonList(new Object[] {2, "second", "p4"}));
        importRows(path, begun, rows);
        try (Table table = connection.getTable(path)) {
            verify(table, PARTITIONED, new Object[] {1, "first", "p1"});
            verify(table, PARTITIONED, new Object[] {2, "second", "p4"});
        }
    }

    @Test
    void testBatchScanReadsImportedRows() throws Exception {
        TablePath path = create("snapshot_batch_scan", false);
        PhysicalTablePath target = PhysicalTablePath.of(path);
        importRows(
                path,
                begin(target),
                Collections.singletonMap(
                        target,
                        Arrays.asList(new Object[] {1, "first"}, new Object[] {2, "second"})));
        try (Table table = connection.getTable(path)) {
            retry(
                    TIMEOUT,
                    () -> {
                        try (BatchScanner scanner = table.newScan().createBatchScanner()) {
                            assertThat(BatchScanUtils.collectRows(scanner))
                                    .extracting(
                                            record ->
                                                    row(
                                                            record.getInt(0),
                                                            record.getString(1).toString()))
                                    .containsExactlyInAnyOrder(row(1, "first"), row(2, "second"));
                        } catch (Exception e) {
                            if (ExceptionUtils.findThrowable(e, NotLeaderOrFollowerException.class)
                                    .isPresent()) {
                                throw new AssertionError("Imported rows are not ready to scan", e);
                            }
                            throw e;
                        }
                    });
        }
    }

    @Test
    void testBeginAndAbortLeaveOrdinaryClientAccessAvailable() throws Exception {
        TablePath path = create("ordinary_access", false);
        BeginBulkLoadResponse begun = begin(PhysicalTablePath.of(path));
        try (Table table = connection.getTable(path)) {
            write(table, row(1, "during-begin"));
            admin.getAdminGateway()
                    .abortBulkLoad(
                            new AbortBulkLoadRequest().setHandle(begun.getStatus().getHandle()))
                    .get();
            verify(table, SCHEMA, new Object[] {1, "during-begin"});
        }
    }

    @ParameterizedTest
    @EnumSource(ChangelogImage.class)
    void testImportedRowsSupportUpdateAndDeleteChangelog(ChangelogImage image) throws Exception {
        TablePath path = create("changelog", false, image);
        BeginBulkLoadResponse begun = begin(PhysicalTablePath.of(path));
        importRows(
                path,
                begun,
                Collections.singletonMap(
                        PhysicalTablePath.of(path),
                        Arrays.asList(new Object[] {1, "imported"}, new Object[] {2, "retained"})));
        long tableId = begun.getStatus().getHandle().getTableId();
        try (Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            for (int id = 0; id < 2; id++) {
                scanner.subscribe(
                        id,
                        cluster.waitAndGetLeaderReplica(new TableBucket(tableId, id))
                                .getLocalLogEndOffset());
            }
            verify(table, SCHEMA, new Object[] {1, "imported"});
            write(table, row(1, "updated"));
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.delete(row(1, (String) null));
            writer.flush();
            boolean sawUpdate = false;
            boolean sawDelete = false;
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while ((!sawUpdate || !sawDelete) && System.nanoTime() < deadline) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (TableBucket bucket : records.buckets()) {
                    for (ScanRecord record : records.records(bucket)) {
                        if (record.getRow().getInt(0) == 1) {
                            if (record.getChangeType() == ChangeType.UPDATE_AFTER) {
                                assertThat(record.getRow().getString(1).toString())
                                        .isEqualTo("updated");
                                sawUpdate = true;
                            }
                            if (record.getChangeType() == ChangeType.DELETE) {
                                if (image == ChangelogImage.FULL) {
                                    assertThat(record.getRow().getString(1).toString())
                                            .isEqualTo("updated");
                                }
                                sawDelete = true;
                            }
                        }
                    }
                }
            }
            assertThat(sawUpdate).isTrue();
            assertThat(sawDelete).isTrue();
            retry(
                    TIMEOUT,
                    () -> {
                        try {
                            assertThat(
                                            table.newLookup()
                                                    .createLookuper()
                                                    .lookup(row(1))
                                                    .get(10, TimeUnit.SECONDS)
                                                    .getSingletonRow())
                                    .isNull();
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    });
            verify(table, SCHEMA, new Object[] {2, "retained"});
        }
    }

    private BeginBulkLoadResponse begin(PhysicalTablePath... targets) throws Exception {
        BeginBulkLoadRequest request = new BeginBulkLoadRequest();
        request.setTablePath()
                .setDatabaseName(targets[0].getTablePath().getDatabaseName())
                .setTableName(targets[0].getTablePath().getTableName());
        for (PhysicalTablePath target : targets) {
            if (target.getPartitionName() != null) {
                request.addPartitionName(target.getPartitionName());
            }
        }
        return admin.getAdminGateway().beginBulkLoad(request).get();
    }

    private void importRows(
            TablePath path,
            BeginBulkLoadResponse begun,
            Map<PhysicalTablePath, List<Object[]>> rows)
            throws Exception {
        new TestingBulkLoadInputBuilder()
                .commit(cluster, admin.getAdminGateway(), path, begun, rows);
    }

    private TablePath create(String name, boolean partitioned) throws Exception {
        return create(name, partitioned, ChangelogImage.FULL);
    }

    private TablePath create(String name, boolean partitioned, ChangelogImage image)
            throws Exception {
        TablePath path = TablePath.of("bulkload_shared", name);
        await(admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true));
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(partitioned ? PARTITIONED : SCHEMA)
                        .property(ConfigOptions.TABLE_CHANGELOG_IMAGE, image)
                        .distributedBy(2, "id");
        if (partitioned) {
            builder.partitionedBy("pt");
        }
        await(admin.createTable(path, builder.build(), false));
        if (partitioned) {
            for (String part : Arrays.asList("p1", "p2", "p3")) {
                await(
                        admin.createPartition(
                                path,
                                new PartitionSpec(Collections.singletonMap("pt", part)),
                                false));
            }
        }
        return path;
    }

    private static void write(Table table, InternalRow row) throws Exception {
        UpsertWriter writer = table.newUpsert().createWriter();
        writer.upsert(row);
        writer.flush();
    }

    private static void verify(Table table, Schema schema, Object[] expected) {
        retry(
                TIMEOUT,
                () -> {
                    try {
                        Lookuper lookuper = table.newLookup().createLookuper();
                        InternalRow key =
                                expected.length == 2
                                        ? row((Integer) expected[0])
                                        : row((Integer) expected[0], expected[2]);
                        InternalRow actual =
                                lookuper.lookup(key).get(10, TimeUnit.SECONDS).getSingletonRow();
                        assertThatRow(actual)
                                .withSchema(schema.getRowType())
                                .isEqualTo(row(schema.getRowType(), expected));
                    } catch (Exception failure) {
                        throw new AssertionError("Imported row is not ready", failure);
                    }
                });
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
