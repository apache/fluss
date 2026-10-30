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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.FetchLogReadPreference;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.ListOffsetsParam;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.delay.DelayedTableBucketKey;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests client log visibility while synchronous index replication is behind the log HW. */
class SyncIndexLogFetchTest extends ReplicaTestBase {

    private static final long TABLE_ID = 720001L;
    private static final TablePath TABLE_PATH = TablePath.of("test_db_1", "index_log_visibility");
    private static final TableBucket BUCKET = new TableBucket(TABLE_ID, 0);
    private static final int FOLLOWER_ID = TABLET_SERVER_ID + 1;

    @Override
    protected Configuration getServerConf() {
        Configuration conf = super.getServerConf();
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        return conf;
    }

    @Test
    void testClientFetchStopsAtSyncProgressWhileFollowerAndIndexReadsContinue() throws Exception {
        Replica replica = createReplica(IndexVisibility.SYNC);
        addMultiSegmentsToLogTablet(replica.getLogTablet(), 2);
        replica.advanceIndexProgress(10L, 0L);

        FetchLogResultForBucket clientResult = fetch(-1, 0L, FetchLogReadPreference.LOCAL_FIRST);
        assertThat(clientResult.records().batches())
                .allSatisfy(batch -> assertThat(batch.nextLogOffset()).isLessThanOrEqualTo(10L));
        assertThat(clientResult.records().sizeInBytes()).isPositive();
        assertThat(fetch(-1, 10L, FetchLogReadPreference.LOCAL_FIRST).records().sizeInBytes())
                .isZero();

        FetchLogResultForBucket followerResult =
                fetch(FOLLOWER_ID, 10L, FetchLogReadPreference.LOCAL_FIRST);
        assertThat(followerResult.records().sizeInBytes()).isPositive();
        assertThat(followerResult.getHighWatermark()).isEqualTo(20L);
        assertThat(
                        replica.getOffset(
                                remoteLogManager,
                                new ListOffsetsParam(
                                        -1, ListOffsetsParam.LATEST_OFFSET_TYPE, null)))
                .isEqualTo(10L);
        assertThat(
                        replica.getOffset(
                                remoteLogManager,
                                new ListOffsetsParam(
                                        FOLLOWER_ID, ListOffsetsParam.LATEST_OFFSET_TYPE, null)))
                .isEqualTo(20L);
        replica.advanceIndexProgress(15L, 0L);
        FetchLogResultForBucket partialSegment = fetch(-1, 10L, FetchLogReadPreference.LOCAL_FIRST);
        assertThat(partialSegment.records().sizeInBytes()).isPositive();
        assertThat(partialSegment.records().batches())
                .allSatisfy(batch -> assertThat(batch.nextLogOffset()).isLessThanOrEqualTo(15L));
        assertThat(
                        replica.getLogTablet()
                                .read(10L, Integer.MAX_VALUE, FetchIsolation.HIGH_WATERMARK, true)
                                .getRecords()
                                .batches())
                .anySatisfy(batch -> assertThat(batch.nextLogOffset()).isGreaterThan(15L));
    }

    @Test
    void testDelayedClientFetchWakesOnlyAfterSyncProgressAdvances() throws Exception {
        Replica replica = createReplica(IndexVisibility.SYNC);
        addMultiSegmentsToLogTablet(replica.getLogTablet(), 1);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> response =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(-1, Integer.MAX_VALUE, 1, Duration.ofMinutes(3).toMillis()),
                Collections.singletonMap(BUCKET, new FetchReqInfo(TABLE_ID, 0L, Integer.MAX_VALUE)),
                null,
                response::complete);
        assertThat(response).isNotDone();
        replica.advanceIndexProgress(0L, 10L);
        replicaManager
                .getDelayedFetchLogManager()
                .checkAndComplete(new DelayedTableBucketKey(BUCKET));
        assertThat(response).isNotDone();

        replica.advanceIndexProgress(10L, 10L);
        assertThat(response.get(10, TimeUnit.SECONDS).get(BUCKET).records().sizeInBytes())
                .isPositive();
    }

    @Test
    void testClientFetchCompletionDoesNotBlockIndexProgressOrDemotion() throws Exception {
        Replica replica = createReplica(IndexVisibility.SYNC);
        addMultiSegmentsToLogTablet(replica.getLogTablet(), 1);
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CompletableFuture<Void> releaseCallback = new CompletableFuture<>();
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> response =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(-1, Integer.MAX_VALUE, 1, Duration.ofMinutes(3).toMillis()),
                Collections.singletonMap(BUCKET, new FetchReqInfo(TABLE_ID, 0L, Integer.MAX_VALUE)),
                null,
                result -> {
                    callbackEntered.countDown();
                    releaseCallback.join();
                    response.complete(result);
                });
        CompletableFuture<Void> progress =
                CompletableFuture.runAsync(() -> replica.advanceIndexProgress(10L, 10L));
        try {
            assertThat(callbackEntered.await(10, TimeUnit.SECONDS)).isTrue();
            // Fetch completion may acquire replica locks or block on the client. It must not
            // run inline on the index progress publisher, which can hold the replicator lock.
            progress.get(10, TimeUnit.SECONDS);
            assertThat(response).isNotDone();
            assertThat(
                            replica.makeFollower(
                                    new NotifyLeaderAndIsrData(
                                            PhysicalTablePath.of(TABLE_PATH),
                                            BUCKET,
                                            Arrays.asList(TABLET_SERVER_ID, FOLLOWER_ID),
                                            new LeaderAndIsr(
                                                    FOLLOWER_ID,
                                                    1,
                                                    Collections.singletonList(FOLLOWER_ID),
                                                    Collections.emptyList(),
                                                    INITIAL_COORDINATOR_EPOCH,
                                                    1))))
                    .isTrue();
        } finally {
            releaseCallback.complete(null);
            progress.get(10, TimeUnit.SECONDS);
        }
        assertThat(response.get(10, TimeUnit.SECONDS).get(BUCKET).records().sizeInBytes())
                .isPositive();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTablesWithoutSyncIndexesStillReadThroughHighWatermark(boolean hasAsyncIndex)
            throws Exception {
        Replica replica = createReplica(hasAsyncIndex ? IndexVisibility.ASYNC : null);
        addMultiSegmentsToLogTablet(replica.getLogTablet(), 1);
        assertThat(fetch(-1, 0L, FetchLogReadPreference.LOCAL_FIRST).records().sizeInBytes())
                .isPositive();
    }

    @ParameterizedTest
    @EnumSource(FetchLogReadPreference.class)
    void testRemoteClientFetchDoesNotExposeUnindexedSegments(FetchLogReadPreference preference)
            throws Exception {
        Replica replica = createReplica(IndexVisibility.SYNC);
        LogTablet log = replica.getLogTablet();
        addMultiSegmentsToLogTablet(log, 5);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        log.updateRemoteLogOffsets(0L, 40L, 40L);
        // A recovered leader may only have the newer local tail, while replay uses remote WAL.
        log.updateMinRetainOffset(40L);
        log.deleteSegmentsAlreadyExistsInRemote();
        assertThat(log.localLogStartOffset()).isGreaterThan(15L);

        assertThat(fetch(-1, 0L, preference).records().sizeInBytes()).isZero();
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> response =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(
                        -1,
                        true,
                        Integer.MAX_VALUE,
                        1,
                        Duration.ofMinutes(3).toMillis(),
                        null,
                        preference),
                Collections.singletonMap(BUCKET, new FetchReqInfo(TABLE_ID, 0L, Integer.MAX_VALUE)),
                null,
                response::complete);
        assertThat(response).isNotDone();
        replica.advanceIndexProgress(15L, 0L);
        assertThat(response.get(10, TimeUnit.SECONDS).get(BUCKET).fetchFromRemote()).isTrue();
        FetchLogResultForBucket result = fetch(-1, 0L, preference);
        assertThat(result.fetchFromRemote()).isTrue();
        assertThat(result.remoteLogFetchInfo().remoteLogSegmentList()).hasSize(1);
        assertThat(result.remoteLogFetchInfo().remoteLogSegmentList())
                .allSatisfy(
                        segment ->
                                assertThat(segment.remoteLogEndOffset()).isLessThanOrEqualTo(15L));
        assertThat(fetch(-1, 10L, preference).records().sizeInBytes()).isZero();

        FetchLogResultForBucket followerResult = fetch(FOLLOWER_ID, 0L, preference);
        assertThat(followerResult.remoteLogFetchInfo().remoteLogSegmentList()).hasSize(4);
        assertThat(followerResult.getHighWatermark()).isEqualTo(50L);
        replica.advanceIndexProgress(40L, 0L);
        assertThat(fetch(-1, 10L, preference).remoteLogFetchInfo().remoteLogSegmentList())
                .hasSize(3);
    }

    private Replica createReplica(@Nullable IndexVisibility visibility) throws Exception {
        Schema.Builder schema =
                Schema.newBuilder().fromColumns(DATA1_SCHEMA_PK.getColumns()).primaryKey("a");
        if (visibility != null) {
            schema.index(
                    "idx_b", IndexType.SECONDARY, Collections.singletonList("b"), visibility, 3);
        }
        registerTableInZkClient(
                TABLE_PATH,
                schema.build(),
                TABLE_ID,
                Collections.singletonList("a"),
                Collections.singletonMap(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "1"));
        // Missing target metadata keeps index progress under deterministic test control.
        makeKvTableAsLeader(
                BUCKET,
                TABLE_PATH,
                Arrays.asList(TABLET_SERVER_ID, FOLLOWER_ID),
                Collections.singletonList(TABLET_SERVER_ID),
                0,
                false);
        return replicaManager.getReplicaOrException(BUCKET);
    }

    private FetchLogResultForBucket fetch(
            int replicaId, long offset, FetchLogReadPreference preference) {
        FetchParams params =
                new FetchParams(replicaId, true, Integer.MAX_VALUE, -1, -1, null, preference);
        FetchLogResultForBucket result =
                replicaManager
                        .readFromLog(
                                params,
                                Collections.singletonMap(
                                        BUCKET,
                                        new FetchReqInfo(TABLE_ID, offset, Integer.MAX_VALUE)),
                                null)
                        .get(BUCKET)
                        .getFetchLogResultForBucket();
        assertThat(result.getError()).isEqualTo(ApiError.NONE);
        return result;
    }
}
