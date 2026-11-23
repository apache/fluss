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
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.admin.GetMvccSnapshotOffsetsResult;
import org.apache.fluss.client.admin.ListMvccReadBoundsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.admin.RegisterMvccSnapshotOffsetsResult;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.client.utils.TestingRpcClient;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidRequestException;
import org.apache.fluss.exception.MvccReadOffsetExpiredException;
import org.apache.fluss.exception.MvccReadOffsetNotYetReadableException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TableNotVersionedException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.BucketLogOffset;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.MvccReadBounds;
import org.apache.fluss.metadata.MvccSnapshotBinding;
import org.apache.fluss.metadata.MvccSnapshotRef;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ListMvccSnapshotBindingsResponse;
import org.apache.fluss.rpc.messages.RegisterMvccSnapshotOffsetsResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.MessageCodec;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.KvTabletTestUtils;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.metadata.MvccSnapshotBindingSchema;
import org.apache.fluss.server.metadata.SystemTables;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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
    void registersAndResolvesNamedSnapshotsThroughSeparateConnections() throws Exception {
        TablePath path = TablePath.of("mvcc_test", "named_snapshot_orders");
        try (Connection writer = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Connection reader = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = writer.getAdmin();
                Admin readerAdmin = reader.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, tableDescriptor(true).withBucketCount(2), false).get();
            try (Table table = writer.getTable(path);
                    Table readerTable = reader.getTable(path)) {
                long tableId = table.getTableInfo().getTableId();
                TableBucket first = new TableBucket(tableId, 0);
                TableBucket second = new TableBucket(tableId, 1);
                CLUSTER.waitUntilAllReplicaReady(first);
                CLUSTER.waitUntilAllReplicaReady(second);
                MvccSnapshotRef snapshot = MvccSnapshotRef.of(tableId, "start");
                assertThat(
                                readerAdmin
                                        .getMvccSnapshotOffsets(
                                                snapshot, Arrays.asList(first, second))
                                        .get(15, TimeUnit.SECONDS)
                                        .getMissingBuckets())
                        .containsExactlyInAnyOrder(first, second);
                assertThat(readerAdmin.listMvccSnapshotBindings(first).get(15, TimeUnit.SECONDS))
                        .isEmpty();
                BucketLogOffset firstOffset = BucketLogOffset.of(first, 0);
                BucketLogOffset secondOffset = BucketLogOffset.of(second, 0);
                assertThatThrownBy(
                                () ->
                                        admin.registerMvccSnapshotOffsets(
                                                        snapshot,
                                                        Arrays.asList(
                                                                firstOffset,
                                                                secondOffset,
                                                                BucketLogOffset.of(first, 1)))
                                                .get(15, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(InvalidRequestException.class);
                assertThat(
                                readerAdmin
                                        .getMvccSnapshotOffsets(
                                                snapshot, Arrays.asList(first, second))
                                        .get(15, TimeUnit.SECONDS)
                                        .getMissingBuckets())
                        .containsExactlyInAnyOrder(first, second);
                RegisterMvccSnapshotOffsetsResult initialResult =
                        admin.registerMvccSnapshotOffsets(
                                        snapshot, Arrays.asList(firstOffset, firstOffset))
                                .get(15, TimeUnit.SECONDS);
                assertThat(initialResult.getErrors()).isEmpty();
                assertThat(initialResult.getRegisteredBuckets()).containsExactly(first);
                GetMvccSnapshotOffsetsResult partial =
                        readerAdmin
                                .getMvccSnapshotOffsets(snapshot, Arrays.asList(first, second))
                                .get(15, TimeUnit.SECONDS);
                assertThat(partial.getOffsets()).containsEntry(first, firstOffset);
                assertThat(partial.getMissingBuckets()).containsExactly(second);
                RegisterMvccSnapshotOffsetsResult mixed =
                        admin.registerMvccSnapshotOffsets(
                                        snapshot,
                                        Arrays.asList(BucketLogOffset.of(first, 43), secondOffset))
                                .get(15, TimeUnit.SECONDS);
                assertThat(mixed.getRegisteredBuckets()).containsExactly(second);
                assertThat(mixed.getConflictingBuckets()).containsExactly(first);
                for (TableBucket bucket : Arrays.asList(first, second)) {
                    assertThat(
                                    readerAdmin
                                            .listMvccSnapshotBindings(bucket, "default")
                                            .get(15, TimeUnit.SECONDS))
                            .containsExactly(
                                    MvccSnapshotBinding.of(
                                            snapshot, BucketLogOffset.of(bucket, 0)));
                }
                Lookuper lookup = readerTable.newLookup().createLookuper();
                // Exactly one resolved bucket owns this key, even when the row does not exist.
                int successfulLookups = 0;
                for (BucketLogOffset candidate :
                        readerAdmin
                                .getMvccSnapshotOffsets(snapshot, Arrays.asList(first, second))
                                .get(15, TimeUnit.SECONDS)
                                .getOffsets()
                                .values()) {
                    try {
                        assertThat(
                                        lookup.lookup(row(1, 1), candidate)
                                                .get(15, TimeUnit.SECONDS)
                                                .getRowList())
                                .isEmpty();
                        successfulLookups++;
                    } catch (IllegalArgumentException e) {
                        // The other bucket does not own this key.
                    } catch (java.util.concurrent.ExecutionException e) {
                        assertThat(e).hasRootCauseInstanceOf(IllegalArgumentException.class);
                    }
                }
                assertThat(successfulLookups).isOne();
            }
        }
    }

    @Test
    void replacesExpiredBindingOnceAndRecoversTheWinnerAfterFailover() throws Exception {
        TablePath path = TablePath.of("mvcc_test", "snapshot_replacement");
        try (FlussConnection writer =
                        (FlussConnection)
                                ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                FlussConnection competitor =
                        (FlussConnection)
                                ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = writer.getAdmin();
                Admin competingAdmin = competitor.getAdmin();
                RpcClient leftWire =
                        RpcClient.create(writer.getConfiguration(), writer.getClientMetricGroup());
                RpcClient rightWire =
                        RpcClient.create(
                                competitor.getConfiguration(), competitor.getClientMetricGroup());
                Admin leftAdmin = dropSuccessfulRegistrationReply(writer, leftWire);
                Admin rightAdmin = dropSuccessfulRegistrationReply(competitor, rightWire)) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, tableDescriptor(true), false).get();
            TableBucket bucket = new TableBucket(admin.getTableInfo(path).get().getTableId(), 0);
            CLUSTER.waitUntilAllReplicaReady(bucket);
            MvccSnapshotRef ref = MvccSnapshotRef.of(bucket.getTableId(), "failover", "reused");
            long oldDeadline = CLOCK.milliseconds() + 1000;
            RegisterMvccSnapshotOffsetsResult initialResult =
                    admin.registerMvccSnapshotOffsets(
                                    ref,
                                    Collections.singleton(BucketLogOffset.of(bucket, 0)),
                                    oldDeadline)
                            .get(15, TimeUnit.SECONDS);
            assertThat(initialResult.getErrors()).isEmpty();
            assertThat(initialResult.getRegisteredBuckets()).containsExactly(bucket);
            CLOCK.advanceTime(Duration.ofSeconds(2));
            assertThat(
                            competingAdmin
                                    .getMvccSnapshotOffsets(ref, Collections.singleton(bucket))
                                    .get(15, TimeUnit.SECONDS)
                                    .getMissingBuckets())
                    .containsExactly(bucket);
            long expiresAt = CLOCK.milliseconds() + 10000;
            CompletableFuture<RegisterMvccSnapshotOffsetsResult> left =
                    leftAdmin.registerMvccSnapshotOffsets(
                            ref, Collections.singleton(BucketLogOffset.of(bucket, 43)), expiresAt);
            CompletableFuture<RegisterMvccSnapshotOffsetsResult> right =
                    rightAdmin.registerMvccSnapshotOffsets(
                            ref, Collections.singleton(BucketLogOffset.of(bucket, 44)), expiresAt);
            boolean leftWon = registrationReplyWasDropped(left, bucket);
            assertThat(registrationReplyWasDropped(right, bucket)).isNotEqualTo(leftWon);
            BucketLogOffset winner = BucketLogOffset.of(bucket, leftWon ? 43 : 44);

            TableInfo bindings = admin.getTableInfo(SystemTables.MVCC_SNAPSHOT_BINDINGS).get();
            int shard =
                    new MvccSnapshotBindingSchema()
                            .shard(bucket, ref.getNamespace(), bindings.getNumBuckets());
            TableBucket systemBucket = new TableBucket(bindings.getTableId(), shard);
            int leader = CLUSTER.waitAndGetLeader(systemBucket);
            awaitInSyncFollowers(systemBucket, leader);
            CLUSTER.stopTabletServer(leader);
            try {
                assertThat(CLUSTER.waitAndGetLeader(systemBucket)).isNotEqualTo(leader);
                try (Connection third =
                                ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                        Admin observer = third.getAdmin()) {
                    retry(
                            Duration.ofSeconds(15),
                            () ->
                                    assertThat(
                                                    observer.getMvccSnapshotOffsets(
                                                                    ref,
                                                                    Collections.singleton(bucket))
                                                            .get(10, TimeUnit.SECONDS)
                                                            .getOffsets())
                                            .containsEntry(bucket, winner));
                    assertThat(
                                    observer.registerMvccSnapshotOffsets(
                                                    ref, Collections.singleton(winner))
                                            .get(15, TimeUnit.SECONDS)
                                            .getRegisteredBuckets())
                            .containsExactly(bucket);
                    CLOCK.advanceTime(Duration.ofSeconds(10));
                    assertThat(
                                    observer.getMvccSnapshotOffsets(
                                                    ref, Collections.singleton(bucket))
                                            .get(15, TimeUnit.SECONDS)
                                            .getMissingBuckets())
                            .containsExactly(bucket);
                }
            } finally {
                CLUSTER.startTabletServer(leader);
            }
        }
    }

    @Test
    void retainsAcknowledgedBucketWhenAnotherSystemShardTimesOut() throws Exception {
        TablePath path = TablePath.of("mvcc_test", "binding_timeout");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, tableDescriptor(true).withBucketCount(2), false).get();
            long tableId = admin.getTableInfo(path).get().getTableId();
            TableBucket first = new TableBucket(tableId, 0);
            TableBucket second = new TableBucket(tableId, 1);
            CLUSTER.waitUntilAllReplicaReady(first);
            CLUSTER.waitUntilAllReplicaReady(second);
            BucketLogOffset a = BucketLogOffset.of(first, 0);
            BucketLogOffset b = BucketLogOffset.of(second, 0);
            RegisterMvccSnapshotOffsetsResult initialize =
                    admin.registerMvccSnapshotOffsets(
                                    MvccSnapshotRef.of(tableId, "initialize"),
                                    Collections.singleton(a))
                            .get(15, TimeUnit.SECONDS);
            assertThat(initialize.getErrors()).isEmpty();
            TableInfo info = admin.getTableInfo(SystemTables.MVCC_SNAPSHOT_BINDINGS).get();
            assertThat(info.getNumBuckets()).isEqualTo(16);
            MvccSnapshotBindingSchema schema = new MvccSnapshotBindingSchema();
            String namespace =
                    IntStream.range(0, 128)
                            .mapToObj(index -> "timeout-" + index)
                            .filter(
                                    candidate ->
                                            schema.shard(first, candidate, info.getNumBuckets())
                                                    != schema.shard(
                                                            second,
                                                            candidate,
                                                            info.getNumBuckets()))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            TableBucket shardA =
                    new TableBucket(
                            info.getTableId(),
                            schema.shard(first, namespace, info.getNumBuckets()));
            TableBucket shardB =
                    new TableBucket(
                            info.getTableId(),
                            schema.shard(second, namespace, info.getNumBuckets()));
            CLUSTER.waitUntilAllReplicaReady(shardA);
            CLUSTER.waitUntilAllReplicaReady(shardB);
            KvTablet blockedTablet = CLUSTER.waitAndGetLeaderReplica(shardB).getKvTablet();
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            pauseSnapshotBindingFlush(blockedTablet, entered, release);
            MvccSnapshotRef snapshot = MvccSnapshotRef.of(tableId, namespace, "pending");
            try {
                CompletableFuture<RegisterMvccSnapshotOffsetsResult> registration =
                        admin.registerMvccSnapshotOffsets(snapshot, Arrays.asList(a, b));
                assertThat(entered.await(15, TimeUnit.SECONDS)).isTrue();
                RegisterMvccSnapshotOffsetsResult outcomes = registration.get(25, TimeUnit.SECONDS);
                assertThat(outcomes.getRegisteredBuckets()).containsExactly(first);
                assertThat(outcomes.getErrors().get(second)).isInstanceOf(TimeoutException.class);
            } finally {
                release.countDown();
                KvTabletTestUtils.setBeforeNativeWrite(blockedTablet, null);
            }
            retry(
                    Duration.ofSeconds(15),
                    () ->
                            assertThat(
                                            admin.getMvccSnapshotOffsets(
                                                            snapshot, Arrays.asList(first, second))
                                                    .get(10, TimeUnit.SECONDS)
                                                    .getOffsets())
                                    .containsEntry(first, a)
                                    .containsEntry(second, b));
        }
    }

    @Test
    void rejectsRegistrationThatExpiresBeforeItsWriteBecomesVisible() throws Exception {
        TablePath path = TablePath.of("mvcc_test", "binding_expires_before_ack");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, tableDescriptor(true), false).get();
            long tableId = admin.getTableInfo(path).get().getTableId();
            TableBucket bucket = new TableBucket(tableId, 0);
            CLUSTER.waitUntilAllReplicaReady(bucket);
            BucketLogOffset offset = BucketLogOffset.of(bucket, 0);
            MvccSnapshotRef initialize = MvccSnapshotRef.of(tableId, "initialize");
            assertThat(
                            admin.registerMvccSnapshotOffsets(
                                            initialize, Collections.singleton(offset))
                                    .get(15, TimeUnit.SECONDS)
                                    .getRegisteredBuckets())
                    .containsExactly(bucket);
            TableInfo info = admin.getTableInfo(SystemTables.MVCC_SNAPSHOT_BINDINGS).get();
            int shard =
                    new MvccSnapshotBindingSchema().shard(bucket, "default", info.getNumBuckets());
            KvTablet tablet =
                    CLUSTER.waitAndGetLeaderReplica(new TableBucket(info.getTableId(), shard))
                            .getKvTablet();
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            pauseSnapshotBindingFlush(tablet, entered, release);
            MvccSnapshotRef snapshot = MvccSnapshotRef.of(tableId, "expires-before-ack");
            try {
                CompletableFuture<RegisterMvccSnapshotOffsetsResult> request =
                        admin.registerMvccSnapshotOffsets(
                                snapshot,
                                Collections.singleton(offset),
                                CLOCK.milliseconds() + 1000);
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                CLOCK.advanceTime(Duration.ofSeconds(2));
                release.countDown();
                RegisterMvccSnapshotOffsetsResult result = request.get(15, TimeUnit.SECONDS);
                assertThat(result.getRegisteredBuckets()).isEmpty();
                assertThat(result.getErrors().get(bucket))
                        .isInstanceOf(InvalidRequestException.class);
                assertThat(
                                admin.getMvccSnapshotOffsets(
                                                snapshot, Collections.singleton(bucket))
                                        .get(15, TimeUnit.SECONDS)
                                        .getMissingBuckets())
                        .containsExactly(bucket);
            } finally {
                release.countDown();
                KvTabletTestUtils.setBeforeNativeWrite(tablet, null);
            }
        }
    }

    private static void pauseSnapshotBindingFlush(
            KvTablet tablet, CountDownLatch entered, CountDownLatch release) {
        KvTabletTestUtils.setBeforeNativeWrite(
                tablet,
                () -> {
                    entered.countDown();
                    try {
                        if (!release.await(30, TimeUnit.SECONDS)) {
                            throw new AssertionError("Test did not release the KV flush");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                });
    }

    @Test
    void listsOnlyWhenTheCompleteResponseFitsTheFrameBudget() throws Exception {
        TablePath path = TablePath.of("mvcc_test", "binding_list_budget");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, tableDescriptor(true), false).get();
            long tableId = admin.getTableInfo(path).get().getTableId();
            TableBucket bucket = new TableBucket(tableId, 0);
            CLUSTER.waitUntilAllReplicaReady(bucket);
            int budget = MvccSnapshotBindingSchema.LIST_RESPONSE_BYTES;
            int header = Integer.BYTES + MessageCodec.RESPONSE_HEADER_LENGTH;
            ListMvccSnapshotBindingsResponse encoded = new ListMvccSnapshotBindingsResponse();
            int nameSize = budget / 4;
            for (char digit = '0'; digit < '4'; digit++) {
                encoded.addBinding().setName(repeatedName(digit, nameSize)).setNextOffset(0L);
            }
            int excess = header + encoded.totalSize() - budget;
            encoded.getBindingsList().get(3).setName(repeatedName('3', nameSize - excess));
            ListMvccSnapshotBindingsResponse exact =
                    new ListMvccSnapshotBindingsResponse()
                            .addAllBindings(encoded.getBindingsList());
            assertThat(header + exact.totalSize()).isEqualTo(budget);

            for (String name :
                    Arrays.asList(
                            repeatedName('0', nameSize),
                            repeatedName('1', nameSize),
                            repeatedName('2', nameSize),
                            repeatedName('3', nameSize - excess))) {
                RegisterMvccSnapshotOffsetsResult result =
                        admin.registerMvccSnapshotOffsets(
                                        MvccSnapshotRef.of(tableId, "budget", name),
                                        Collections.singleton(BucketLogOffset.of(bucket, 0)))
                                .get(20, TimeUnit.SECONDS);
                assertThat(result.getErrors()).isEmpty();
                assertThat(result.getRegisteredBuckets()).containsExactly(bucket);
            }
            assertThat(admin.listMvccSnapshotBindings(bucket, "budget").get(20, TimeUnit.SECONDS))
                    .hasSize(4);
            RegisterMvccSnapshotOffsetsResult overflow =
                    admin.registerMvccSnapshotOffsets(
                                    MvccSnapshotRef.of(tableId, "budget", "extra"),
                                    Collections.singleton(BucketLogOffset.of(bucket, 0)))
                            .get(20, TimeUnit.SECONDS);
            assertThat(overflow.getErrors()).isEmpty();
            assertThatThrownBy(
                            () ->
                                    admin.listMvccSnapshotBindings(bucket, "budget")
                                            .get(20, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(InvalidRequestException.class);
        }
    }

    private static String repeatedName(char character, int length) {
        char[] name = new char[length];
        Arrays.fill(name, character);
        return new String(name);
    }

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
                TableBucket invalid = new TableBucket(bucket.getTableId(), Integer.MAX_VALUE);
                ListMvccReadBoundsResult emptyBounds =
                        admin.listMvccReadBounds(Arrays.asList(bucket, invalid, bucket));
                assertThat(emptyBounds.bucketResult(bucket).get().getEarliestOffset()).isZero();
                assertThat(emptyBounds.bucketResult(bucket).get().getLatestOffset()).isZero();
                assertThatThrownBy(() -> emptyBounds.bucketResult(invalid).get())
                        .hasRootCauseInstanceOf(InvalidRequestException.class);
                assertThatThrownBy(() -> emptyBounds.all().get())
                        .hasRootCauseInstanceOf(InvalidRequestException.class);

                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(row(1, 1, 10)).get(30, TimeUnit.SECONDS);
                writer.upsert(row(1, 2, 20)).get(30, TimeUnit.SECONDS);
                assertThat(prefix.lookup(row(1), BucketLogOffset.of(bucket, 0)).get().getRowList())
                        .isEmpty();
                assertThat(prefix.lookup(row(1), BucketLogOffset.of(bucket, 1)).get().getRowList())
                        .extracting(r -> r.getInt(2))
                        .containsExactly(10);
                BucketLogOffset earlier =
                        BucketLogOffset.of(
                                bucket, readBounds(admin, bucket).get().getLatestOffset());

                MvccSnapshotRef boundHistory =
                        MvccSnapshotRef.of(bucket.getTableId(), "older-data");
                assertThat(
                                admin.registerMvccSnapshotOffsets(
                                                boundHistory, Collections.singleton(earlier))
                                        .get(15, TimeUnit.SECONDS)
                                        .getRegisteredBuckets())
                        .containsExactly(bucket);
                CLOCK.advanceTime(Duration.ofHours(2));
                writer.upsert(row(1, 1, 30)).get(30, TimeUnit.SECONDS);
                BucketLogOffset afterUpdateBefore =
                        BucketLogOffset.of(bucket, earlier.getOffset() + 1);
                assertThat(
                                point.lookup(row(1, 1), afterUpdateBefore)
                                        .get()
                                        .getSingletonRow()
                                        .getInt(2))
                        .isEqualTo(10);
                writer.delete(row(1, 2, 20)).get(30, TimeUnit.SECONDS);
                long endBeforeMissingDelete = readBounds(admin, bucket).get().getLatestOffset();
                writer.delete(row(1, 99, 0)).get(30, TimeUnit.SECONDS);
                MvccReadBounds range = readBounds(admin, bucket).get();
                assertThat(range.getLatestOffset()).isEqualTo(endBeforeMissingDelete + 1);
                assertThat(
                                prefix.lookup(
                                                row(1),
                                                BucketLogOffset.of(bucket, range.getLatestOffset()))
                                        .get()
                                        .getRowList())
                        .extracting(r -> r.getInt(2))
                        .containsExactly(30);
                assertThat(point.lookup(row(1, 1)).get().getSingletonRow().getInt(2)).isEqualTo(30);
                assertThat(point.lookup(row(1, 1), earlier).get().getSingletonRow().getInt(2))
                        .isEqualTo(10);
                assertThat(prefix.lookup(row(1), earlier).get().getRowList())
                        .extracting(r -> r.getInt(2))
                        .containsExactlyInAnyOrder(10, 20);
                long futureOffset = range.getLatestOffset() + 1;
                assertThatThrownBy(
                                () ->
                                        point.lookup(
                                                        row(1, 1),
                                                        BucketLogOffset.of(bucket, futureOffset))
                                                .get())
                        .cause()
                        .isInstanceOfSatisfying(
                                MvccReadOffsetNotYetReadableException.class,
                                e ->
                                        assertThat(e.getLatestReadableOffset())
                                                .isEqualTo(
                                                        BucketLogOffset.of(
                                                                bucket, range.getLatestOffset())));

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
                                readBounds(admin, bucket)
                                                .get(10, TimeUnit.SECONDS)
                                                .getEarliestOffset()
                                        == afterShrink.getHistoryFloor(),
                        Duration.ofSeconds(30),
                        "Leader did not publish the snapshot's history floor");
                assertExpired(prefix, earlier, afterShrink.getHistoryFloor());
                assertThat(
                                admin.getMvccSnapshotOffsets(
                                                boundHistory, Collections.singleton(bucket))
                                        .get(15, TimeUnit.SECONDS)
                                        .getOffsets())
                        .containsEntry(bucket, earlier);
                int leader = CLUSTER.waitAndGetLeader(bucket);
                awaitInSyncFollowers(bucket, leader);
                CLUSTER.stopTabletServer(leader);
                try {
                    assertThat(CLUSTER.waitAndGetLeader(bucket)).isNotEqualTo(leader);
                    BucketLogOffset retained =
                            BucketLogOffset.of(
                                    bucket, readBounds(admin, bucket).get().getEarliestOffset());
                    assertThat(retained)
                            .isEqualTo(BucketLogOffset.of(bucket, afterShrink.getHistoryFloor()));
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
                BucketLogOffset earlier = BucketLogOffset.of(bucket, 1);
                CLUSTER.triggerAndWaitSnapshot(bucket);
                CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet().roll(Optional.empty());
                CLOCK.advanceTime(Duration.ofHours(2));
                writer.upsert(row(1, 1, 20)).get(30, TimeUnit.SECONDS);
                CompletedSnapshot current = CLUSTER.triggerAndWaitSnapshot(bucket);
                assertThat(current.getLogOffset()).isGreaterThan(earlier.getOffset());
                waitUntil(
                        () ->
                                admin.listOffsets(
                                                        path,
                                                        Collections.singleton(0),
                                                        new OffsetSpec.EarliestSpec())
                                                .bucketResult(0)
                                                .get(10, TimeUnit.SECONDS)
                                        >= earlier.getOffset(),
                        Duration.ofMinutes(1),
                        "Changelog carrying the first value did not expire");

                BucketLogOffset checkpointPosition =
                        BucketLogOffset.of(bucket, current.getLogOffset());
                writer.upsert(row(1, 1, 40)).get(30, TimeUnit.SECONDS);
                BucketLogOffset replayed =
                        BucketLogOffset.of(
                                bucket, readBounds(admin, bucket).get().getLatestOffset());
                assertThat(replayed.getOffset()).isGreaterThan(current.getLogOffset());

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
                                () -> point.lookup(row(1, 1), BucketLogOffset.of(bucket, 0)).get())
                        .hasRootCauseInstanceOf(TableNotVersionedException.class);
                assertThatThrownBy(() -> readBounds(admin, bucket).get())
                        .hasRootCauseInstanceOf(TableNotVersionedException.class);

                admin.dropTable(path, false).get();
                retry(
                        Duration.ofSeconds(30),
                        () ->
                                assertThatThrownBy(
                                                () ->
                                                        readBounds(admin, bucket)
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
            BucketLogOffset original;
            try (Table table = connection.getTable(path)) {
                long partitionId = admin.listPartitionInfos(path).get().get(0).getPartitionId();
                TableBucket bucket =
                        new TableBucket(table.getTableInfo().getTableId(), partitionId, 0);
                Lookuper prefix = table.newLookup().lookupBy("customer", "region").createLookuper();
                table.newUpsert()
                        .createWriter()
                        .upsert(row(1, 1, 10, "eu"))
                        .get(30, TimeUnit.SECONDS);
                original =
                        BucketLogOffset.of(
                                bucket, readBounds(admin, bucket).get().getLatestOffset());
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
                                                    readBounds(admin, original.getTableBucket())
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
                BucketLogOffset replacementPosition =
                        BucketLogOffset.of(
                                replacementBucket,
                                readBounds(admin, replacementBucket).get().getLatestOffset());
                assertThat(replacementPosition.getOffset()).isEqualTo(original.getOffset());
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
                                        readBounds(admin, original.getTableBucket())
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

    private static void assertExpired(Lookuper prefix, BucketLogOffset position, long floor) {
        assertThatThrownBy(() -> prefix.lookup(row(1), position).get())
                .cause()
                .isInstanceOfSatisfying(
                        MvccReadOffsetExpiredException.class,
                        error ->
                                assertThat(error.getEarliestReadableOffset())
                                        .isEqualTo(
                                                BucketLogOffset.of(
                                                        position.getTableBucket(), floor)));
    }

    private static Admin dropSuccessfulRegistrationReply(
            FlussConnection connection, RpcClient wire) {
        return new FlussAdmin(
                new TestingRpcClient(
                        (node, api) -> {
                            throw new AssertionError("The test must use the real wire");
                        }) {
                    @Override
                    public CompletableFuture<ApiMessage> sendRequest(
                            ServerNode node, ApiKeys api, ApiMessage request) {
                        return wire.sendRequest(node, api, request)
                                .thenApply(
                                        reply -> {
                                            if (api == ApiKeys.REGISTER_MVCC_SNAPSHOT_OFFSETS
                                                    && ((RegisterMvccSnapshotOffsetsResponse) reply)
                                                            .getResultsList().stream()
                                                                    .anyMatch(
                                                                            item ->
                                                                                    !item
                                                                                            .hasError())) {
                                                throw new NetworkException(
                                                        "Registration response lost at the client boundary");
                                            }
                                            return reply;
                                        });
                    }
                },
                connection.getMetadataUpdater());
    }

    private static boolean registrationReplyWasDropped(
            CompletableFuture<RegisterMvccSnapshotOffsetsResult> registration, TableBucket bucket)
            throws Exception {
        try {
            assertThat(registration.get(15, TimeUnit.SECONDS).getConflictingBuckets())
                    .containsExactly(bucket);
            return false;
        } catch (ExecutionException e) {
            assertThat(e).hasRootCauseInstanceOf(NetworkException.class);
            return true;
        }
    }

    private static CompletableFuture<MvccReadBounds> readBounds(Admin admin, TableBucket bucket) {
        return admin.listMvccReadBounds(Collections.singleton(bucket)).bucketResult(bucket);
    }
}
