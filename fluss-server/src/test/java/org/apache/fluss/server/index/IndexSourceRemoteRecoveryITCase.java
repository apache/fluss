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
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.TableAssignment;

import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.fluss.server.index.TestingIndexTable.INDEX_NAME;
import static org.apache.fluss.server.index.TestingIndexTable.deleteRequest;
import static org.apache.fluss.server.index.TestingIndexTable.indexEntry;
import static org.apache.fluss.server.index.TestingIndexTable.upsertRequest;
import static org.apache.fluss.server.index.TestingIndexTable.valueForIndexBucket;
import static org.apache.fluss.server.index.TestingIndexTable.writeSource;
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
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration REMOTE_TIMEOUT = Duration.ofMinutes(2);

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(TABLET_SERVER_COUNT)
                    .setClusterConf(configuration())
                    .build();

    @Test
    void recoverAsyncIndexFromRawRemoteSourceWalAndContinueLocally() throws Throwable {
        RemoteRecoveryFixture recovery = createRecoveryFixture();
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

    private static Configuration configuration() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, REPLICATION_FACTOR);
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("100b"));
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.setInt(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS, 1);
        conf.set(ConfigOptions.INDEX_REPLICATION_SOURCE_MAX_BYTES, MemorySize.parse("1b"));
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        conf.set(ConfigOptions.INDEX_REPLICATION_RETRY_BACKOFF_MAX, Duration.ofMillis(100));
        return conf;
    }

    private static RemoteRecoveryFixture createRecoveryFixture() throws Exception {
        String tableName = "source_remote_recovery_" + System.nanoTime();
        TablePath mainPath = TablePath.of("index_remote_recovery", tableName);
        Schema schema = TestingIndexTable.indexedSchema(IndexVisibility.ASYNC, INDEX_BUCKET_COUNT);
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
            List<Integer> sourceReplicas =
                    mainAssignment.getBucketAssignment(sourceBucket).getReplicas();
            for (int indexBucket = 0; indexBucket < INDEX_BUCKET_COUNT; indexBucket++) {
                List<Integer> indexReplicas =
                        indexAssignment.getBucketAssignment(indexBucket).getReplicas();
                if (Collections.disjoint(sourceReplicas, indexReplicas)) {
                    return new RemoteRecoveryFixture(
                            table,
                            new TableBucket(mainTableId, sourceBucket),
                            new TableBucket(indexTableId, indexBucket),
                            sourceReplicas,
                            indexReplicas);
                }
            }
        }
        throw new AssertionError("The RF3 assignments on six servers contain no disjoint pair");
    }

    private static long persistBaselineSnapshot(
            RemoteRecoveryFixture recovery, Map<Integer, String> expectedRows) throws Exception {
        CLUSTER.stopTabletServer(recovery.recoveryFollower);
        CLUSTER.waitUntilReplicaShrinkFromIsr(
                recovery.sourceTableBucket, recovery.recoveryFollower);
        CLUSTER.stopTabletServer(recovery.offlineFollower);
        CLUSTER.waitUntilReplicaShrinkFromIsr(recovery.sourceTableBucket, recovery.offlineFollower);

        for (int key = 0; key < BASELINE_ROW_COUNT; key++) {
            putSourceRow(recovery, recovery.sourceLeader, key, recovery.firstIndexValue);
            expectedRows.put(key, recovery.firstIndexValue);
        }
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

        assertExactIndexProjection(recovery, expectedRows);
        CompletedSnapshot snapshot = CLUSTER.triggerAndWaitSnapshot(recovery.sourceTableBucket);
        assertThat(snapshot.getLogOffset()).isEqualTo(baselinePushedOffset);
        assertThat(snapshot.getAllIndexPushedOffset())
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
            CLUSTER.stopTabletServer(targetReplica);
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
        writeSource(
                CLUSTER.newTabletServerClientForNode(recovery.sourceLeader),
                deleteRequest(recovery.sourceTableBucket, UPDATED_ROW_COUNT)
                        .setTimeoutMs((int) TIMEOUT.toMillis()));
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
                recovery.sourceReplica, baselinePushedOffset, committedSourceEnd);
        return committedSourceEnd;
    }

    private static Replica recoverFromRemoteWal(
            RemoteRecoveryFixture recovery, long baselinePushedOffset, long committedSourceEnd)
            throws Exception {
        CLUSTER.startTabletServer(recovery.recoveryFollower);
        CLUSTER.waitUntilReplicaExpandToIsr(recovery.sourceTableBucket, recovery.recoveryFollower);
        Replica recoveryFollowerReplica =
                CLUSTER.waitAndGetFollowerReplica(
                        recovery.sourceTableBucket, recovery.recoveryFollower);
        long followerLocalStart = recoveryFollowerReplica.getLogTablet().localLogStartOffset();
        assertThat(followerLocalStart)
                .as("the selected recovery follower must have discarded the baseline range")
                .isGreaterThan(baselinePushedOffset);

        CLUSTER.stopTabletServer(recovery.sourceLeader);
        waitUntil(
                () ->
                        CLUSTER.waitAndGetLeader(recovery.sourceTableBucket)
                                == recovery.recoveryFollower,
                TIMEOUT,
                "wait for the exact remote-recovery follower to lead the source bucket");
        Replica newSourceReplica = CLUSTER.waitAndGetLeaderReplica(recovery.sourceTableBucket);
        assertThat(CLUSTER.waitAndGetLeader(recovery.sourceTableBucket))
                .isEqualTo(recovery.recoveryFollower);
        assertThat(newSourceReplica.getLogTablet().localLogStartOffset())
                .as("the replay start must be unavailable from local source WAL")
                .isGreaterThan(baselinePushedOffset);
        assertThat(newSourceReplica.getAllIndexPushedOffset())
                .as("the unavailable target must preserve the snapshot-restored baseline")
                .isEqualTo(baselinePushedOffset);
        return newSourceReplica;
    }

    private static void verifyRecoveredProjection(
            RemoteRecoveryFixture recovery,
            Replica recovered,
            long committedSourceEnd,
            Map<Integer, String> expectedRows)
            throws Exception {
        for (int targetReplica : recovery.targetReplicas) {
            CLUSTER.startTabletServer(targetReplica);
        }
        CLUSTER.waitAndGetLeaderReplica(recovery.gatedTargetBucket);
        waitUntil(
                () -> recovered.getAllIndexPushedOffset() == committedSourceEnd,
                TIMEOUT,
                "wait for exact recovered source replay progress");
        assertThat(recovered.getAllIndexPushedOffset()).isEqualTo(committedSourceEnd);
        CLUSTER.startTabletServer(recovery.sourceLeader);
        CLUSTER.startTabletServer(recovery.offlineFollower);
        CLUSTER.assertHasTabletServerNumber(TABLET_SERVER_COUNT);
        assertExactIndexProjection(recovery, expectedRows);
    }

    private static void verifyLocalContinuation(
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
    }

    private static void putSourceRow(
            RemoteRecoveryFixture recovery, int sourceLeader, int key, String indexedValue)
            throws Exception {
        writeSource(
                CLUSTER.newTabletServerClientForNode(sourceLeader),
                upsertRequest(recovery.sourceTableBucket, key, indexedValue)
                        .setTimeoutMs((int) TIMEOUT.toMillis()));
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
            Replica sourceReplica, long baselinePushedOffset, long committedSourceEnd) {
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

    private static void assertExactIndexProjection(
            RemoteRecoveryFixture recovery, Map<Integer, String> expectedRows) throws Exception {
        Map<Integer, Map<String, String>> expected = new LinkedHashMap<>();
        for (Map.Entry<Integer, String> sourceRow : expectedRows.entrySet()) {
            IndexReplicationEncoder.IndexEntry entry =
                    indexEntry(recovery.indexEncoder, sourceRow.getKey(), sourceRow.getValue());
            byte[] value =
                    ValueEncoder.encodeValue(
                            (short) recovery.indexEncoder.getIndexSchemaId(), entry.value());
            String previous =
                    expected.computeIfAbsent(entry.targetBucket(), ignored -> new LinkedHashMap<>())
                            .put(encode(entry.key()), encode(value));
            assertThat(previous).as("physical index keys must be unique").isNull();
        }
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            TableBucket tableBucket = new TableBucket(recovery.indexTableId, bucket);
            Map<String, String> expectedKeys =
                    expected.getOrDefault(bucket, Collections.emptyMap());
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
                        String key = encode(iterator.key());
                        assertThat(expectedKeys.remove(key))
                                .as("physical index key %s in %s", key, tableBucket)
                                .isEqualTo(encode(iterator.value()));
                    }
                    iterator.next();
                }
                iterator.status();
            }
        }
        assertThat(expected.values())
                .as("missing physical index entries")
                .allSatisfy(entries -> assertThat(entries).isEmpty());
    }

    private static String encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static final class RemoteRecoveryFixture {
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

        private RemoteRecoveryFixture(
                TestingIndexTable table,
                TableBucket sourceBucket,
                TableBucket targetBucket,
                List<Integer> sourceReplicas,
                List<Integer> targetReplicas)
                throws Exception {
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
            List<Integer> followers =
                    sourceReplicas.stream()
                            .filter(server -> server != sourceLeader)
                            .collect(Collectors.toList());
            assertThat(followers).hasSize(2);
            this.recoveryFollower = followers.get(0);
            this.offlineFollower = followers.get(1);
            this.targetReplicas = targetReplicas;
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
    }
}
