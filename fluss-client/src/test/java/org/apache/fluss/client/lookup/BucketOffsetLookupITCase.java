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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.LookupPositionExpiredException;
import org.apache.fluss.exception.LookupPositionNotYetReadableException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TableNotVersionedException;
import org.apache.fluss.metadata.BucketReadPosition;
import org.apache.fluss.metadata.BucketReadRange;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.ZkData.PartitionIdZNode;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises positioned reads through the client and the actual TabletServer RPCs. */
class BucketOffsetLookupITCase {
    private static final ManualClock CLOCK = new ManualClock(System.currentTimeMillis());

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConfig())
                    .setClock(CLOCK)
                    .build();

    @Test
    void readsDurablePositionsAndExpiresHistoryInAnIdleBucket() throws Exception {
        TablePath path = TablePath.of("mvcc_test", "orders");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, tableDescriptor(true), false).get();
            try (Table table = connection.getTable(path)) {
                TableBucket bucket = new TableBucket(table.getTableInfo().getTableId(), 0);
                CLUSTER.waitUntilAllReplicaReady(bucket);
                Lookuper point = table.newLookup().createLookuper();
                Lookuper prefix = table.newLookup().lookupBy("customer").createLookuper();
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(row(1, 1, 10)).get(30, TimeUnit.SECONDS);
                writer.upsert(row(1, 2, 20)).get(30, TimeUnit.SECONDS);
                assertThat(
                                prefix.lookup(row(1), BucketReadPosition.of(bucket, 0))
                                        .get()
                                        .getRowList())
                        .isEmpty();
                assertThat(
                                prefix.lookup(row(1), BucketReadPosition.of(bucket, 1))
                                        .get()
                                        .getRowList())
                        .extracting(r -> r.getInt(2))
                        .containsExactly(10);
                BucketReadPosition earlier =
                        admin.getBucketReadRange(bucket).get().getLatestPosition();

                CLOCK.advanceTime(Duration.ofHours(2));
                writer.upsert(row(1, 1, 30)).get(30, TimeUnit.SECONDS);
                BucketReadPosition afterUpdateBefore =
                        BucketReadPosition.of(bucket, earlier.getNextOffset() + 1);
                assertThat(
                                point.lookup(row(1, 1), afterUpdateBefore)
                                        .get()
                                        .getSingletonRow()
                                        .getInt(2))
                        .isEqualTo(10);
                writer.delete(row(1, 2, 20)).get(30, TimeUnit.SECONDS);
                long endBeforeMissingDelete =
                        admin.getBucketReadRange(bucket).get().getLatestPosition().getNextOffset();
                writer.delete(row(1, 99, 0)).get(30, TimeUnit.SECONDS);
                BucketReadRange range = admin.getBucketReadRange(bucket).get();
                assertThat(range.getLatestPosition().getNextOffset())
                        .isEqualTo(endBeforeMissingDelete + 1);
                assertThat(prefix.lookup(row(1), range.getLatestPosition()).get().getRowList())
                        .extracting(r -> r.getInt(2))
                        .containsExactly(30);
                assertThat(point.lookup(row(1, 1)).get().getSingletonRow().getInt(2)).isEqualTo(30);
                assertThat(point.lookup(row(1, 1), earlier).get().getSingletonRow().getInt(2))
                        .isEqualTo(10);
                assertThat(prefix.lookup(row(1), earlier).get().getRowList())
                        .extracting(r -> r.getInt(2))
                        .containsExactlyInAnyOrder(10, 20);
                long futureOffset = range.getLatestPosition().getNextOffset() + 1;
                assertThatThrownBy(
                                () ->
                                        point.lookup(
                                                        row(1, 1),
                                                        BucketReadPosition.of(bucket, futureOffset))
                                                .get())
                        .cause()
                        .isInstanceOfSatisfying(
                                LookupPositionNotYetReadableException.class,
                                e ->
                                        assertThat(e.getLatestPosition())
                                                .isEqualTo(range.getLatestPosition()));

                CompletedSnapshot beforeShrink = CLUSTER.triggerAndWaitSnapshot(bucket);
                assertThat(scanSnapshot(table, bucket, beforeShrink.getSnapshotID()))
                        .containsExactly(30);
                CLOCK.advanceTime(Duration.ofHours(2));
                admin.alterTable(
                                path,
                                Collections.singletonList(
                                        TableChange.set(
                                                ConfigOptions.TABLE_KV_MVCC_TTL.key(), "1 h")),
                                false)
                        .get();
                waitUntil(
                        () ->
                                CLUSTER.waitAndGetLeaderReplica(bucket)
                                        .getTableInfo()
                                        .getTableConfig()
                                        .getMvccTtl()
                                        .equals(Duration.ofHours(1)),
                        Duration.ofSeconds(30),
                        "Leader did not apply the history TTL");
                CompletedSnapshot afterShrink = CLUSTER.triggerAndWaitSnapshot(bucket);
                assertThat(afterShrink.getLogOffset()).isEqualTo(beforeShrink.getLogOffset());
                assertThat(afterShrink.getHistoryFloor()).isEqualTo(afterShrink.getLogOffset());
                waitUntil(
                        () ->
                                admin.getBucketReadRange(bucket)
                                                .get(10, TimeUnit.SECONDS)
                                                .getEarliestPosition()
                                                .getNextOffset()
                                        == afterShrink.getHistoryFloor(),
                        Duration.ofSeconds(30),
                        "Leader did not publish the snapshot's history floor");
                assertExpired(prefix, earlier, afterShrink.getHistoryFloor());
                int leader = CLUSTER.waitAndGetLeader(bucket);
                awaitInSyncFollowers(bucket, leader);
                CLUSTER.stopTabletServer(leader);
                try {
                    assertThat(CLUSTER.waitAndGetLeader(bucket)).isNotEqualTo(leader);
                    BucketReadPosition retained =
                            admin.getBucketReadRange(bucket).get().getEarliestPosition();
                    assertThat(retained)
                            .isEqualTo(
                                    BucketReadPosition.of(bucket, afterShrink.getHistoryFloor()));
                    assertExpired(prefix, earlier, afterShrink.getHistoryFloor());
                    assertThat(prefix.lookup(row(1), retained).get().getRowList())
                            .extracting(r -> r.getInt(2))
                            .containsExactly(30);
                } finally {
                    CLUSTER.startTabletServer(leader);
                }
            }
        }
    }

    @Test
    void recoversHistoryAfterItsChangelogHasExpired() throws Exception {
        TablePath path = TablePath.of("mvcc_test", "short_changelog");
        TableDescriptor versioned = tableDescriptor(true);
        Map<String, String> properties = new HashMap<>(versioned.getProperties());
        properties.put(ConfigOptions.TABLE_LOG_TTL.key(), "1 h");
        TableDescriptor descriptor = versioned.withProperties(properties);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, descriptor, false).get();
            try (Table table = connection.getTable(path)) {
                TableBucket bucket = new TableBucket(table.getTableInfo().getTableId(), 0);
                CLUSTER.waitUntilAllReplicaReady(bucket);
                Lookuper point = table.newLookup().createLookuper();
                Lookuper prefix = table.newLookup().lookupBy("customer").createLookuper();
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(row(1, 1, 10)).get(30, TimeUnit.SECONDS);
                BucketReadPosition earlier = BucketReadPosition.of(bucket, 1);
                CLUSTER.triggerAndWaitSnapshot(bucket);
                CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet().roll(Optional.empty());
                CLOCK.advanceTime(Duration.ofHours(2));
                writer.upsert(row(1, 1, 20)).get(30, TimeUnit.SECONDS);
                CompletedSnapshot current = CLUSTER.triggerAndWaitSnapshot(bucket);
                assertThat(current.getLogOffset()).isGreaterThan(earlier.getNextOffset());
                waitUntil(
                        () ->
                                admin.listOffsets(
                                                        path,
                                                        Collections.singleton(0),
                                                        new OffsetSpec.EarliestSpec())
                                                .bucketResult(0)
                                                .get(10, TimeUnit.SECONDS)
                                        >= earlier.getNextOffset(),
                        Duration.ofMinutes(1),
                        "Changelog carrying the first value did not expire");

                BucketReadPosition checkpointPosition =
                        BucketReadPosition.of(bucket, current.getLogOffset());
                writer.upsert(row(1, 1, 40)).get(30, TimeUnit.SECONDS);
                BucketReadPosition replayed =
                        admin.getBucketReadRange(bucket).get().getLatestPosition();
                assertThat(replayed.getNextOffset()).isGreaterThan(current.getLogOffset());

                int leader = CLUSTER.waitAndGetLeader(bucket);
                awaitInSyncFollowers(bucket, leader);
                CLUSTER.stopTabletServer(leader);
                try {
                    assertThat(CLUSTER.waitAndGetLeader(bucket)).isNotEqualTo(leader);
                    assertThat(point.lookup(row(1, 1), earlier).get().getSingletonRow().getInt(2))
                            .isEqualTo(10);
                    assertThat(prefix.lookup(row(1), earlier).get().getSingletonRow().getInt(2))
                            .isEqualTo(10);
                    assertThat(
                                    point.lookup(row(1, 1), checkpointPosition)
                                            .get()
                                            .getSingletonRow()
                                            .getInt(2))
                            .isEqualTo(20);
                    assertThat(point.lookup(row(1, 1), replayed).get().getSingletonRow().getInt(2))
                            .isEqualTo(40);
                } finally {
                    CLUSTER.startTabletServer(leader);
                }
            }
        }
    }

    @Test
    void ordinaryTablesRejectPositionedReads() throws Exception {
        TablePath path = TablePath.of("mvcc_test", "ordinary_orders");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, tableDescriptor(false), false).get();
            assertThatThrownBy(
                            () ->
                                    admin.alterTable(
                                                    path,
                                                    Collections.singletonList(
                                                            TableChange.set(
                                                                    ConfigOptions
                                                                            .TABLE_KV_MVCC_ENABLED
                                                                            .key(),
                                                                    "true")),
                                                    false)
                                            .get())
                    .hasRootCauseInstanceOf(InvalidAlterTableException.class);
            try (Table table = connection.getTable(path)) {
                TableBucket bucket = new TableBucket(table.getTableInfo().getTableId(), 0);
                Lookuper point = table.newLookup().createLookuper();
                assertThat(point.lookup(row(1, 1)).get().getRowList()).isEmpty();
                assertThatThrownBy(
                                () ->
                                        point.lookup(row(1, 1), BucketReadPosition.of(bucket, 0))
                                                .get())
                        .hasRootCauseInstanceOf(TableNotVersionedException.class);
                assertThatThrownBy(() -> admin.getBucketReadRange(bucket).get())
                        .hasRootCauseInstanceOf(TableNotVersionedException.class);

                admin.dropTable(path, false).get();
                retry(
                        Duration.ofSeconds(30),
                        () ->
                                assertThatThrownBy(
                                                () ->
                                                        admin.getBucketReadRange(bucket)
                                                                .get(10, TimeUnit.SECONDS))
                                        .hasRootCauseInstanceOf(TableNotExistException.class));
            }
        }
    }

    @Test
    void doesNotReadAReplacementPartitionAtAnOldPosition() throws Exception {
        TablePath path = TablePath.of("mvcc_test", "partitioned_orders");
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("customer", DataTypes.INT())
                                        .column("order", DataTypes.INT())
                                        .column("amount", DataTypes.INT())
                                        .column("region", DataTypes.STRING())
                                        .primaryKey("customer", "order", "region")
                                        .build())
                        .partitionedBy("region")
                        .distributedBy(1, "customer")
                        .property(ConfigOptions.TABLE_KV_MVCC_ENABLED, true)
                        .build();
        PartitionSpec region = new PartitionSpec(Collections.singletonMap("region", "eu"));
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, descriptor, false).get();
            admin.createPartition(path, region, false).get();
            BucketReadPosition original;
            try (Table table = connection.getTable(path)) {
                long partitionId = admin.listPartitionInfos(path).get().get(0).getPartitionId();
                TableBucket bucket =
                        new TableBucket(table.getTableInfo().getTableId(), partitionId, 0);
                Lookuper prefix = table.newLookup().lookupBy("customer", "region").createLookuper();
                table.newUpsert()
                        .createWriter()
                        .upsert(row(1, 1, 10, "eu"))
                        .get(30, TimeUnit.SECONDS);
                original = admin.getBucketReadRange(bucket).get().getLatestPosition();
                assertThat(prefix.lookup(row(1, "eu"), original).get().getSingletonRow().getInt(2))
                        .isEqualTo(10);
            }

            CuratorFramework curator = CLUSTER.getZooKeeperClient().getCuratorClient();
            String originalAssignmentPath =
                    PartitionIdZNode.path(original.getTableBucket().getPartitionId());
            byte[] originalAssignment = curator.getData().forPath(originalAssignmentPath);
            admin.dropPartition(path, region, false).get();
            admin.createPartition(path, region, false).get();
            long replacementPartitionId =
                    admin.listPartitionInfos(path).get().get(0).getPartitionId();
            assertThat(replacementPartitionId)
                    .isNotEqualTo(original.getTableBucket().getPartitionId());
            retry(
                    Duration.ofSeconds(30),
                    () ->
                            assertThatThrownBy(
                                            () ->
                                                    admin.getBucketReadRange(
                                                                    original.getTableBucket())
                                                            .get(10, TimeUnit.SECONDS))
                                    .hasRootCauseInstanceOf(PartitionNotExistException.class));
            try (Connection fresh = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                    Table replacement = fresh.getTable(path)) {
                TableBucket replacementBucket =
                        new TableBucket(
                                replacement.getTableInfo().getTableId(), replacementPartitionId, 0);
                CLUSTER.waitUntilAllReplicaReady(replacementBucket);
                replacement
                        .newUpsert()
                        .createWriter()
                        .upsert(row(1, 1, 20, "eu"))
                        .get(30, TimeUnit.SECONDS);
                Lookuper prefix =
                        replacement.newLookup().lookupBy("customer", "region").createLookuper();
                BucketReadPosition replacementPosition =
                        admin.getBucketReadRange(replacementBucket).get().getLatestPosition();
                assertThat(replacementPosition.getNextOffset()).isEqualTo(original.getNextOffset());
                assertThat(
                                prefix.lookup(row(1, "eu"), replacementPosition)
                                        .get()
                                        .getSingletonRow()
                                        .getInt(2))
                        .isEqualTo(20);
                assertThatThrownBy(() -> prefix.lookup(row(1, "eu"), original).get())
                        .hasRootCauseInstanceOf(IllegalArgumentException.class);
            }

            waitUntil(
                    () -> curator.checkExists().forPath(originalAssignmentPath) == null,
                    Duration.ofSeconds(30),
                    "The original partition assignment was not cleaned up");
            // An assignment may remain after the old partition is removed from routing metadata.
            curator.create().forPath(originalAssignmentPath, originalAssignment);
            try {
                assertThatThrownBy(
                                () ->
                                        admin.getBucketReadRange(original.getTableBucket())
                                                .get(10, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(PartitionNotExistException.class);
            } finally {
                curator.delete().forPath(originalAssignmentPath);
            }
        }
    }

    private static List<Integer> scanSnapshot(Table table, TableBucket bucket, long snapshotId)
            throws Exception {
        List<Integer> amounts = new ArrayList<>();
        try (BatchScanner scanner = table.newScan().createBatchScanner(bucket, snapshotId)) {
            CloseableIterator<InternalRow> batch;
            while ((batch = scanner.pollBatch(Duration.ofSeconds(10))) != null) {
                try (CloseableIterator<InternalRow> rows = batch) {
                    while (rows.hasNext()) {
                        amounts.add(rows.next().getInt(2));
                    }
                }
            }
        }
        return amounts;
    }

    private static Configuration clusterConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofHours(12));
        conf.set(ConfigOptions.LOG_RETENTION_CHECK_INTERVAL, Duration.ofSeconds(1));
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("1kb"));
        return conf;
    }

    private static void awaitInSyncFollowers(TableBucket bucket, int leader) {
        for (int replica = 0; replica < 3; replica++) {
            if (replica != leader) {
                CLUSTER.waitUntilReplicaExpandToIsr(bucket, replica);
            }
        }
        assertThat(CLUSTER.waitLeaderAndIsrReady(bucket).isr()).hasSize(3);
    }

    private static TableDescriptor tableDescriptor(boolean versioned) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("customer", DataTypes.INT())
                                        .column("order", DataTypes.INT())
                                        .column("amount", DataTypes.INT())
                                        .primaryKey("customer", "order")
                                        .build())
                        .distributedBy(1, "customer");
        if (versioned) {
            builder.property(ConfigOptions.TABLE_KV_MVCC_ENABLED, true);
        }
        return builder.build();
    }

    private static void assertExpired(Lookuper prefix, BucketReadPosition position, long floor) {
        assertThatThrownBy(() -> prefix.lookup(row(1), position).get())
                .cause()
                .isInstanceOfSatisfying(
                        LookupPositionExpiredException.class,
                        error ->
                                assertThat(error.getEarliestPosition())
                                        .isEqualTo(
                                                BucketReadPosition.of(
                                                        position.getTableBucket(), floor)));
    }
}
