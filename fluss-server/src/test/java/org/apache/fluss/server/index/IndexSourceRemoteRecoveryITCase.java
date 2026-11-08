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

package org.apache.fluss.server.index;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.types.Tuple2;

import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end recovery of an asynchronous index from raw remote source WAL. */
class IndexSourceRemoteRecoveryITCase {

    private static final int TABLET_SERVER_COUNT = 6;
    private static final int REPLICATION_FACTOR = 3;
    private static final int SOURCE_BUCKET_COUNT = 11;
    private static final int INDEX_BUCKET_COUNT = 20;
    private static final int BASELINE_ROW_COUNT = 12;
    private static final int UPDATED_ROW_COUNT = 6;
    private static final int REPLAY_INSERT_COUNT = 12;
    private static final int NEVER_WRITTEN_KEY = 10_000;
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration REMOTE_TIMEOUT = Duration.ofMinutes(2);

    private static final String INDEX_NAME = "idx_b";

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(TABLET_SERVER_COUNT)
                    .setClusterConf(configuration())
                    .build();

    @Test
    void recoverAsyncIndexFromRawRemoteSourceWalAndContinueLocally() throws Throwable {
        try (RemoteRecoveryFixture recovery = createRecoveryFixture()) {
            Map<Integer, String> expectedRows = new LinkedHashMap<>();
            long baseline = persistBaselineSnapshot(recovery, expectedRows);
            long committedEnd = createRemoteReplayRange(recovery, baseline, expectedRows);
            Replica recovered = recoverFromRemoteWal(recovery, baseline, committedEnd);

            int newKey = BASELINE_ROW_COUNT + REPLAY_INSERT_COUNT;
            putSourceRow(recovery, recovery.recoveryFollower, newKey, recovery.firstIndexValue);
            expectedRows.put(newKey, recovery.firstIndexValue);
            waitForSourceCommit(recovered);
            long replayEnd = recovered.getLocalLogEndOffset();
            assertThat(replayEnd).isGreaterThan(committedEnd);
            assertThat(recovered.getAllIndexPushedOffset()).isEqualTo(baseline);

            verifyRecoveredProjection(recovery, recovered, replayEnd, expectedRows);
            verifyLocalContinuation(recovery, recovered, replayEnd, expectedRows);
        }
    }

    private static Configuration configuration() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, REPLICATION_FACTOR);
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("100b"));
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.setInt(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS, 1);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        conf.set(ConfigOptions.INDEX_REPLICATION_RETRY_BACKOFF_MAX, Duration.ofMillis(100));
        return conf;
    }

    private static RemoteRecoveryFixture createRecoveryFixture() throws Exception {
        String tableName = "source_remote_recovery_" + System.nanoTime();
        TablePath mainPath = TablePath.of("index_remote_recovery", tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .index(
                                INDEX_NAME,
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                IndexVisibility.ASYNC,
                                INDEX_BUCKET_COUNT)
                        .build();
        TestingIndexTable table =
                new TestingIndexTable(
                        CLUSTER,
                        mainPath,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(SOURCE_BUCKET_COUNT, "a")
                                .build(),
                        INDEX_NAME);
        long mainTableId = table.mainTableId;
        long indexTableId = table.indexTableId;
        TableAssignment mainAssignment =
                CLUSTER.getZooKeeperClient()
                        .getTableAssignment(mainTableId)
                        .orElseThrow(() -> new AssertionError("Source assignment missing from ZK"));
        TableAssignment indexAssignment =
                CLUSTER.getZooKeeperClient()
                        .getTableAssignment(indexTableId)
                        .orElseThrow(() -> new AssertionError("Index assignment missing from ZK"));
        for (int sourceBucket = 0; sourceBucket < SOURCE_BUCKET_COUNT; sourceBucket++) {
            Set<Integer> sourceReplicas =
                    new LinkedHashSet<>(
                            mainAssignment.getBucketAssignment(sourceBucket).getReplicas());
            for (int indexBucket = 0; indexBucket < INDEX_BUCKET_COUNT; indexBucket++) {
                Set<Integer> indexReplicas =
                        new LinkedHashSet<>(
                                indexAssignment.getBucketAssignment(indexBucket).getReplicas());
                if (Collections.disjoint(sourceReplicas, indexReplicas)) {
                    return new RemoteRecoveryFixture(
                            table,
                            new TableBucket(mainTableId, sourceBucket),
                            new TableBucket(indexTableId, indexBucket));
                }
            }
        }
        throw new AssertionError("The RF3 assignments on six servers contain no disjoint pair");
    }

    private static long persistBaselineSnapshot(
            RemoteRecoveryFixture recovery, Map<Integer, String> expectedRows) throws Exception {
        recovery.stopServer(recovery.recoveryFollower);
        CLUSTER.waitUntilReplicaShrinkFromIsr(
                recovery.sourceTableBucket, recovery.recoveryFollower);
        recovery.stopServer(recovery.offlineFollower);
        CLUSTER.waitUntilReplicaShrinkFromIsr(recovery.sourceTableBucket, recovery.offlineFollower);

        for (int key = 0; key < BASELINE_ROW_COUNT; key++) {
            putSourceRow(recovery, recovery.sourceLeader, key, recovery.firstIndexValue);
            expectedRows.put(key, recovery.firstIndexValue);
        }
        waitForExactPhysicalRows(recovery, expectedRows);
        waitUntil(
                () ->
                        recovery.sourceReplica.getAllIndexPushedOffset()
                                == recovery.sourceReplica.getLocalLogEndOffset(),
                TIMEOUT,
                "wait for the exact baseline index prefix");
        long baselinePushedOffset = recovery.sourceReplica.getLocalLogEndOffset();
        assertThat(baselinePushedOffset).isPositive();
        assertThat(recovery.sourceReplica.getAllIndexPushedOffset())
                .as("the conservative baseline must cover the complete first source prefix")
                .isEqualTo(baselinePushedOffset);

        CompletedSnapshot snapshot = CLUSTER.triggerAndWaitSnapshot(recovery.sourceTableBucket);
        assertThat(snapshot.getLogOffset()).isEqualTo(baselinePushedOffset);
        assertThat(snapshot.getIndexPushedOffset())
                .as("the source snapshot must persist the exact conservative index prefix")
                .isEqualTo(baselinePushedOffset);
        return baselinePushedOffset;
    }

    private static long createRemoteReplayRange(
            RemoteRecoveryFixture recovery,
            long baselinePushedOffset,
            Map<Integer, String> expectedRows)
            throws Exception {
        for (int targetReplica : recovery.targetReplicas) {
            recovery.stopServer(targetReplica);
        }

        for (int key = 0; key < UPDATED_ROW_COUNT; key++) {
            putSourceRow(recovery, recovery.sourceLeader, key, recovery.secondIndexValue);
            expectedRows.put(key, recovery.secondIndexValue);
        }
        int replayInsertEnd = BASELINE_ROW_COUNT + REPLAY_INSERT_COUNT;
        for (int key = BASELINE_ROW_COUNT; key < replayInsertEnd; key++) {
            putSourceRow(recovery, recovery.sourceLeader, key, recovery.secondIndexValue);
            expectedRows.put(key, recovery.secondIndexValue);
        }
        writeSourceRow(recovery, recovery.sourceLeader, UPDATED_ROW_COUNT, null);
        expectedRows.remove(UPDATED_ROW_COUNT);
        waitForSourceCommit(recovery.sourceReplica);
        long committedSourceEnd = recovery.sourceReplica.getLocalLogEndOffset();
        assertThat(committedSourceEnd).isGreaterThan(baselinePushedOffset);
        assertThat(recovery.sourceReplica.getAllIndexPushedOffset())
                .as("an unavailable target must preserve the snapshot replay floor")
                .isEqualTo(baselinePushedOffset);

        LogTablet sourceLog = recovery.sourceReplica.getLogTablet();
        sourceLog.roll(Optional.empty());
        assertThat(sourceLog.activeLogSegment().getBaseOffset())
                .as("the explicit roll must close the complete committed replay range")
                .isEqualTo(committedSourceEnd);
        waitForRawRemoteReplayCoverage(
                recovery, recovery.sourceReplica, baselinePushedOffset, committedSourceEnd);
        return committedSourceEnd;
    }

    private static Replica recoverFromRemoteWal(
            RemoteRecoveryFixture recovery, long baselinePushedOffset, long committedSourceEnd)
            throws Exception {
        recovery.startServer(recovery.recoveryFollower);
        CLUSTER.waitUntilReplicaExpandToIsr(recovery.sourceTableBucket, recovery.recoveryFollower);
        Replica recoveryFollowerReplica =
                CLUSTER.waitAndGetFollowerReplica(
                        recovery.sourceTableBucket, recovery.recoveryFollower);
        long followerLocalStart = recoveryFollowerReplica.getLogTablet().localLogStartOffset();
        assertThat(followerLocalStart)
                .as("the selected recovery follower must have discarded the baseline range")
                .isGreaterThan(baselinePushedOffset);

        TabletServer recoveryTabletServer = sourceTabletServer(recovery.recoveryFollower);
        TabletServerMetricGroup recoveryMetrics =
                recoveryTabletServer.getReplicaManager().getServerMetricGroup();
        long remoteBytesBefore = recoveryMetrics.indexReplicationSourceBytes().getCount();

        recovery.stopServer(recovery.sourceLeader);
        waitUntil(
                () ->
                        CLUSTER.waitAndGetLeader(recovery.sourceTableBucket)
                                == recovery.recoveryFollower,
                TIMEOUT,
                "wait for the exact remote-recovery follower to lead the source bucket");
        Replica newSourceReplica = CLUSTER.waitAndGetLeaderReplica(recovery.sourceTableBucket);
        assertThat(CLUSTER.waitAndGetLeader(recovery.sourceTableBucket))
                .isEqualTo(recovery.recoveryFollower);
        waitUntil(
                () -> recoveryMetrics.indexReplicationSourceBytes().getCount() > remoteBytesBefore,
                TIMEOUT,
                "wait for recovered source to consume remote WAL");
        long remoteBytesWhileTargetUnavailable =
                recoveryMetrics.indexReplicationSourceBytes().getCount();
        assertThat(newSourceReplica.getLogTablet().localLogStartOffset())
                .as("the replay start must be unavailable from local source WAL")
                .isGreaterThan(baselinePushedOffset);
        assertThat(newSourceReplica.getAllIndexPushedOffset())
                .as("the unavailable target must preserve the snapshot-restored baseline")
                .isEqualTo(baselinePushedOffset);
        assertThat(remoteBytesWhileTargetUnavailable)
                .as("raw remote source bytes must be consumed before target progress advances")
                .isGreaterThan(remoteBytesBefore);
        return newSourceReplica;
    }

    private static void verifyRecoveredProjection(
            RemoteRecoveryFixture recovery,
            Replica recovered,
            long committedSourceEnd,
            Map<Integer, String> expectedRows)
            throws Exception {
        for (int targetReplica : new ArrayList<>(recovery.targetReplicas)) {
            recovery.startServer(targetReplica);
        }
        CLUSTER.waitAndGetLeaderReplica(recovery.gatedTargetBucket);
        waitUntil(
                () -> recovered.getAllIndexPushedOffset() == committedSourceEnd,
                TIMEOUT,
                "wait for exact recovered source replay progress");
        assertThat(recovered.getAllIndexPushedOffset()).isEqualTo(committedSourceEnd);
        recovery.startServer(recovery.sourceLeader);
        recovery.startServer(recovery.offlineFollower);
        CLUSTER.assertHasTabletServerNumber(TABLET_SERVER_COUNT);
        assertExactIndexProjection(recovery, expectedRows);
        assertIndexKeyAbsent(recovery, recovery.firstIndexValue, 0, "known stale pre-update key");
        assertIndexKeyAbsent(
                recovery, recovery.secondIndexValue, NEVER_WRITTEN_KEY, "never-written index key");
    }

    private static long verifyLocalContinuation(
            RemoteRecoveryFixture recovery,
            Replica recovered,
            long committedSourceEnd,
            Map<Integer, String> expectedRows)
            throws Exception {
        int continuationKey = BASELINE_ROW_COUNT + REPLAY_INSERT_COUNT + 1;
        putSourceRow(
                recovery, recovery.recoveryFollower, continuationKey, recovery.firstIndexValue);
        expectedRows.put(continuationKey, recovery.firstIndexValue);
        waitForSourceCommit(recovered);
        long continuationEnd = recovered.getLocalLogEndOffset();
        assertThat(continuationEnd).isGreaterThan(committedSourceEnd);
        waitUntil(
                () -> recovered.getAllIndexPushedOffset() == continuationEnd,
                TIMEOUT,
                "wait for exact local continuation progress");
        assertThat(recovered.getAllIndexPushedOffset()).isEqualTo(continuationEnd);
        assertExactIndexProjection(recovery, expectedRows);
        return continuationEnd;
    }

    private static void putSourceRow(
            RemoteRecoveryFixture recovery, int sourceLeader, int key, String indexedValue)
            throws Exception {
        writeSourceRow(recovery, sourceLeader, key, new Object[] {key, indexedValue});
    }

    private static void writeSourceRow(
            RemoteRecoveryFixture recovery, int sourceLeader, int key, @Nullable Object[] value)
            throws Exception {
        TabletServerGateway gateway = CLUSTER.newTabletServerClientForNode(sourceLeader);
        PutKvResponse response =
                gateway.putKv(
                                newPutKvRequest(
                                                recovery.mainTableId,
                                                recovery.sourceTableBucket.getBucket(),
                                                1,
                                                genKvRecordBatch(
                                                        Collections.singletonList(
                                                                Tuple2.of(
                                                                        new Object[] {key},
                                                                        value))))
                                        .setTimeoutMs((int) TIMEOUT.toMillis()))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(response.getBucketsRespsList())
                .allSatisfy(
                        bucket ->
                                assertThat(bucket.hasErrorCode())
                                        .as(
                                                "source mutation key=%s error=%s",
                                                key,
                                                bucket.hasErrorMessage()
                                                        ? bucket.getErrorMessage()
                                                        : "none")
                                        .isFalse());
    }

    private static void waitForSourceCommit(Replica sourceReplica) {
        waitUntil(
                () ->
                        sourceReplica.getLogTablet().getHighWatermark()
                                == sourceReplica.getLocalLogEndOffset(),
                TIMEOUT,
                "wait for source high watermark to reach local end");
        assertThat(sourceReplica.getLogTablet().getHighWatermark())
                .isEqualTo(sourceReplica.getLocalLogEndOffset());
    }

    private static void waitForRawRemoteReplayCoverage(
            RemoteRecoveryFixture recovery,
            Replica sourceReplica,
            long baselinePushedOffset,
            long committedSourceEnd) {
        LogTablet sourceLog = sourceReplica.getLogTablet();
        waitUntil(
                () ->
                        sourceLog.canFetchFromRemoteLog(baselinePushedOffset)
                                && sourceLog.canFetchFromRemoteLog(committedSourceEnd - 1L),
                REMOTE_TIMEOUT,
                "wait for raw remote WAL range ["
                        + baselinePushedOffset
                        + ", "
                        + committedSourceEnd
                        + ")");
    }

    private static void waitForExactPhysicalRows(
            RemoteRecoveryFixture recovery, Map<Integer, String> expectedRows) throws Exception {
        for (Map.Entry<Integer, String> expectedRow : expectedRows.entrySet()) {
            PhysicalIndexRow physical =
                    physicalIndexRow(
                            recovery.indexEncoder, expectedRow.getKey(), expectedRow.getValue());
            TableBucket target = new TableBucket(recovery.indexTableId, physical.targetBucket);
            waitUntil(
                    () -> {
                        org.apache.fluss.utils.ByteArraySlice value =
                                CLUSTER.waitAndGetLeaderReplica(target)
                                        .lookups(Collections.singletonList(physical.key))
                                        .get(0);
                        return value != null && Arrays.equals(value.toByteArray(), physical.value);
                    },
                    TIMEOUT,
                    "wait for exact physical Index Table row " + expectedRow);
        }
    }

    private static void assertExactIndexProjection(
            RemoteRecoveryFixture recovery, Map<Integer, String> expectedRows) throws Exception {
        Map<TableBucket, Map<String, String>> expected = emptyIndexProjection(recovery);
        for (Map.Entry<Integer, String> sourceRow : expectedRows.entrySet()) {
            PhysicalIndexRow physical =
                    physicalIndexRow(
                            recovery.indexEncoder, sourceRow.getKey(), sourceRow.getValue());
            TableBucket bucket = new TableBucket(recovery.indexTableId, physical.targetBucket);
            String previous =
                    expected.get(bucket).put(encode(physical.key), encode(physical.value));
            assertThat(previous).as("physical index keys must be unique").isNull();
        }

        Map<TableBucket, Map<String, String>> actual = emptyIndexProjection(recovery);
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            TableBucket tableBucket = new TableBucket(recovery.indexTableId, bucket);
            byte[] progressKey = recovery.indexEncoder.encodeProgress(bucket, 0L).key();
            Replica replica = CLUSTER.waitAndGetLeaderReplica(tableBucket);
            org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait(
                    replica.getKvTablet(), Long.MAX_VALUE);
            try (ReadOptions readOptions = new ReadOptions();
                    RocksIterator iterator =
                            replica.getKvTablet()
                                    .getRocksDBKv()
                                    .getDb()
                                    .newIterator(
                                            replica.getKvTablet()
                                                    .getRocksDBKv()
                                                    .getDefaultColumnFamilyHandle(),
                                            readOptions)) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    if (!Arrays.equals(iterator.key(), progressKey)) {
                        actual.get(tableBucket)
                                .put(encode(iterator.key()), encode(iterator.value()));
                    }
                    iterator.next();
                }
                iterator.status();
            }
        }
        assertThat(actual)
                .as("every physical Index Table key/value byte must exactly project source state")
                .isEqualTo(expected);
    }

    private static void assertIndexKeyAbsent(
            RemoteRecoveryFixture recovery, String indexedValue, int primaryKey, String description)
            throws Exception {
        PhysicalIndexRow physical =
                physicalIndexRow(recovery.indexEncoder, primaryKey, indexedValue);
        TableBucket target = new TableBucket(recovery.indexTableId, physical.targetBucket);
        assertThat(
                        CLUSTER.waitAndGetLeaderReplica(target)
                                .lookups(Collections.singletonList(physical.key))
                                .get(0))
                .as(description)
                .isNull();
    }

    private static Map<TableBucket, Map<String, String>> emptyIndexProjection(
            RemoteRecoveryFixture recovery) {
        Map<TableBucket, Map<String, String>> projection = new LinkedHashMap<>();
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            projection.put(new TableBucket(recovery.indexTableId, bucket), new LinkedHashMap<>());
        }
        return projection;
    }

    private static PhysicalIndexRow physicalIndexRow(
            IndexReplicationEncoder indexEncoder, int primaryKey, String indexedValue) {
        IndexReplicationEncoder.IndexEntry entry =
                indexEncoder.encodeDataEntry(GenericRow.of(primaryKey, fromString(indexedValue)));
        return new PhysicalIndexRow(
                entry.key(),
                ValueEncoder.encodeValue((short) indexEncoder.getIndexSchemaId(), entry.value()),
                entry.targetBucket());
    }

    private static String valueForIndexBucket(
            IndexReplicationEncoder indexEncoder, String prefix, int targetBucket) {
        for (int suffix = 0; suffix < 100_000; suffix++) {
            String candidate = prefix + '-' + suffix;
            if (physicalIndexRow(indexEncoder, 0, candidate).targetBucket == targetBucket) {
                return candidate;
            }
        }
        throw new AssertionError("No value found for Index Table bucket " + targetBucket);
    }

    private static String encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static TabletServer sourceTabletServer(int serverId) {
        TabletServer tabletServer = CLUSTER.getTabletServerById(serverId);
        assertThat(tabletServer).as("TabletServer %s must be live", serverId).isNotNull();
        return tabletServer;
    }

    private static void stopAndTrack(int serverId, Set<Integer> stoppedServers) throws Exception {
        assertThat(stoppedServers).doesNotContain(serverId);
        CLUSTER.stopTabletServer(serverId);
        assertThat(stoppedServers.add(serverId)).isTrue();
    }

    private static void startAndUntrack(int serverId, Set<Integer> stoppedServers)
            throws Exception {
        assertThat(stoppedServers).contains(serverId);
        CLUSTER.startTabletServer(serverId);
        assertThat(stoppedServers.remove(serverId)).isTrue();
    }

    private static void cleanup(List<Throwable> failures, CleanupAction action) {
        try {
            action.run();
        } catch (Throwable failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            failures.add(failure);
        }
    }

    private static void reportCleanupFailures(List<Throwable> cleanupFailures) {
        if (cleanupFailures.isEmpty()) {
            return;
        }
        AssertionError cleanupFailure =
                new AssertionError("source remote-recovery IT cleanup failed");
        cleanupFailures.forEach(cleanupFailure::addSuppressed);
        throw cleanupFailure;
    }

    @FunctionalInterface
    private interface CleanupAction {
        void run() throws Exception;
    }

    private static final class PhysicalIndexRow {
        private final byte[] key;
        private final byte[] value;
        private final int targetBucket;

        private PhysicalIndexRow(byte[] key, byte[] value, int targetBucket) {
            this.key = key;
            this.value = value;
            this.targetBucket = targetBucket;
        }
    }

    private static final class RemoteRecoveryFixture implements AutoCloseable {
        private final long mainTableId;
        private final long indexTableId;
        private final IndexReplicationEncoder indexEncoder;
        private final TableBucket sourceTableBucket;
        private final int sourceLeader;
        private final int recoveryFollower;
        private final int offlineFollower;
        private final List<Integer> targetReplicas;
        private final TableBucket gatedTargetBucket;
        private final String firstIndexValue;
        private final String secondIndexValue;
        private final Replica sourceReplica;
        private final Set<Integer> stoppedServers = new LinkedHashSet<>();

        private RemoteRecoveryFixture(
                TestingIndexTable table, TableBucket sourceBucket, TableBucket targetBucket)
                throws Exception {
            this.mainTableId = sourceBucket.getTableId();
            this.indexTableId = targetBucket.getTableId();
            this.sourceTableBucket = sourceBucket;
            this.gatedTargetBucket = targetBucket;
            CLUSTER.waitUntilAllReplicaReady(sourceBucket);
            CLUSTER.waitUntilAllReplicaReady(targetBucket);
            this.sourceLeader = CLUSTER.waitAndGetLeader(sourceBucket);
            this.sourceReplica = CLUSTER.waitAndGetLeaderReplica(sourceBucket);
            waitUntil(
                    () -> sourceReplica.getIsr().size() == REPLICATION_FACTOR,
                    TIMEOUT,
                    "wait for source RF3 ISR");
            List<Integer> sourceReplicas =
                    CLUSTER.getZooKeeperClient()
                            .getTableAssignment(mainTableId)
                            .get()
                            .getBucketAssignment(sourceBucket.getBucket())
                            .getReplicas();
            List<Integer> followers =
                    sourceReplicas.stream()
                            .filter(server -> server != sourceLeader)
                            .collect(Collectors.toList());
            assertThat(followers).hasSize(2);
            this.recoveryFollower = followers.get(0);
            this.offlineFollower = followers.get(1);
            this.targetReplicas =
                    new ArrayList<>(
                            CLUSTER.getZooKeeperClient()
                                    .getTableAssignment(indexTableId)
                                    .get()
                                    .getBucketAssignment(targetBucket.getBucket())
                                    .getReplicas());
            assertThat(targetReplicas)
                    .hasSize(REPLICATION_FACTOR)
                    .doesNotContainAnyElementsOf(sourceReplicas)
                    .contains(CLUSTER.waitAndGetLeader(targetBucket));
            this.indexEncoder = table.indexEncoder(sourceBucket);
            this.firstIndexValue =
                    valueForIndexBucket(indexEncoder, "remote-old", targetBucket.getBucket());
            this.secondIndexValue =
                    valueForIndexBucket(indexEncoder, "remote-new", targetBucket.getBucket());
        }

        private void stopServer(int serverId) throws Exception {
            stopAndTrack(serverId, stoppedServers);
        }

        private void startServer(int serverId) throws Exception {
            startAndUntrack(serverId, stoppedServers);
        }

        @Override
        public void close() {
            List<Throwable> failures = new ArrayList<>();
            for (int stoppedServer : new LinkedHashSet<>(stoppedServers)) {
                cleanup(failures, () -> startAndUntrack(stoppedServer, stoppedServers));
            }
            cleanup(failures, () -> CLUSTER.assertHasTabletServerNumber(TABLET_SERVER_COUNT));
            reportCleanupFailures(failures);
        }
    }
}
