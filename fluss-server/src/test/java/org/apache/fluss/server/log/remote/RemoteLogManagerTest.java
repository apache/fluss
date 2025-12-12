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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.utils.FlussPaths.remoteLogDir;
import static org.apache.fluss.utils.FlussPaths.remoteLogTabletDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RemoteLogManager}. */
class RemoteLogManagerTest extends RemoteLogTestBase {

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testBecomeLeaderWithoutRemoteLogManifest(boolean partitionTable) throws Exception {
        TableBucket tb = makeTableBucket(partitionTable);
        assertThatThrownBy(() -> remoteLogManager.relevantRemoteLogSegments(tb, 0L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RemoteLogTablet can't be found for table-bucket " + tb);

        // make leader, and then remote log tablet should be created.
        makeLogTableAsLeader(tb, partitionTable);
        RemoteLogTablet remoteLogTablet = remoteLogManager.remoteLogTablet(tb);
        assertThat(remoteLogTablet).isNotNull();
        assertThat(remoteLogTablet.allRemoteLogSegments()).isEmpty();
        assertThat(remoteLogTablet.getRemoteLogStartOffset()).isEqualTo(Long.MAX_VALUE);
        assertThat(remoteLogTablet.getRemoteLogEndOffset()).isNotPresent();

        // verify upload log segment to remote.
        addMultiSegmentsToLogTablet(replicaManager.getReplicaOrException(tb).getLogTablet(), 5);
        // trigger RLMTask copy local log segment to remote and update metadata.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> remoteLogSegmentList =
                remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        // should upload 4 segments to remote, only 1 active segment left
        assertThat(remoteLogSegmentList.size()).isEqualTo(4);
        assertThat(remoteLogManager.lookupPositionForOffset(remoteLogSegmentList.get(0), 2L))
                .isGreaterThan(10);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testBecomeLeaderWithRemoteLogLogManifest(boolean partitionTable) throws Exception {
        TableBucket tb = makeTableBucket(partitionTable);
        // Need to make leader by ReplicaManager.
        makeLogTableAsLeader(tb, partitionTable);
        addMultiSegmentsToLogTablet(replicaManager.getReplicaOrException(tb).getLogTablet(), 5);
        // trigger RLMTask copy local log segment to remote and update metadata.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        List<RemoteLogSegment> remoteLogSegmentList =
                remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(remoteLogSegmentList.size()).isEqualTo(4);
        assertThat(remoteLogManager.lookupPositionForOffset(remoteLogSegmentList.get(0), 2L))
                .isGreaterThan(10);
        // check remote storage has the files
        assertThat(listRemoteLogFiles(tb))
                .isEqualTo(
                        remoteLogSegmentList.stream()
                                .map(s -> s.remoteLogSegmentId().toString())
                                .collect(Collectors.toSet()));

        // rebuild a remote log manager.
        replicaManager.shutdown();
        replicaManager = buildReplicaManager(new TestCoordinatorGateway());
        makeLogTableAsLeader(tb, partitionTable);
        // trigger reload remote log metadata from remote snapshot.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        remoteLogSegmentList = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(remoteLogSegmentList.size()).isEqualTo(4);
        assertThat(remoteLogManager.lookupPositionForOffset(remoteLogSegmentList.get(0), 2L))
                .isGreaterThan(10);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCopyAndExpireLogSegmentsSuccess(boolean partitionTable) throws Exception {
        long ts1 = manualClock.milliseconds();
        TableBucket tb = makeTableBucket(partitionTable);
        // Need to make leader by ReplicaManager.
        makeLogTableAsLeader(tb, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5, false);

        // should upload 4 segments to remote, only 1 active segment left
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        assertThat(remoteLog.allRemoteLogSegments())
                .hasSize(4)
                .allSatisfy(s -> assertThat(s.maxTimestamp()).isEqualTo(ts1));

        // write 4 segments after 4 days
        manualClock.advanceTime(Duration.ofDays(4));
        long ts2 = manualClock.milliseconds();
        addMultiSegmentsToLogTablet(logTablet, 4, false);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(remoteLog.allRemoteLogSegments())
                .hasSize(8)
                .filteredOn(s -> ts1 == s.maxTimestamp())
                .hasSize(5);
        assertThat(remoteLog.allRemoteLogSegments())
                .filteredOn(s -> ts2 == s.maxTimestamp())
                .hasSize(3);

        // should clean up beginning 5 segments after 4+4 days (ttl=7days)
        manualClock.advanceTime(Duration.ofDays(4));
        addMultiSegmentsToLogTablet(logTablet, 1, false);

        // trigger to upload and clean up
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(remoteLog.allRemoteLogSegments())
                .hasSize(4)
                .filteredOn(s -> ts1 == s.maxTimestamp())
                .hasSize(0);
        assertThat(remoteLog.allRemoteLogSegments())
                .filteredOn(s -> ts2 == s.maxTimestamp())
                .hasSize(4);
        // check remote storage has the files
        assertThat(listRemoteLogFiles(tb))
                .isEqualTo(
                        remoteLog.allRemoteLogSegments().stream()
                                .map(s -> s.remoteLogSegmentId().toString())
                                .collect(Collectors.toSet()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitCopyLogSegmentsToRemoteFailed1(boolean partitionTable) throws Exception {
        // make commit remote log manifest fail by failed to upload remote log manifest
        remoteLogStorage.writeManifestFail.set(true);
        TableBucket tb = makeTableBucket(partitionTable);
        // Need to make leader by ReplicaManager.
        makeLogTableAsLeader(tb, partitionTable);
        addMultiSegmentsToLogTablet(replicaManager.getReplicaOrException(tb).getLogTablet(), 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        // no remote log segment should be committed.
        List<RemoteLogSegment> remoteLogSegmentList =
                remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(remoteLogSegmentList).isEmpty();
        // log storage should clean up the temporary data
        assertThat(listRemoteLogFiles(tb)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitCopyLogSegmentsToRemoteFailed2(boolean partitionTable) throws Exception {
        TableBucket tb = makeTableBucket(partitionTable);
        // Need to make leader by ReplicaManager.
        makeLogTableAsLeader(tb, partitionTable);
        addMultiSegmentsToLogTablet(replicaManager.getReplicaOrException(tb).getLogTablet(), 5);

        // make commit remote log manifest fail by failed to commit remote log manifest
        testCoordinatorGateway.commitRemoteLogManifestFail.set(true);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        // no remote log segment should be committed.
        List<RemoteLogSegment> remoteLogSegmentList =
                remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(remoteLogSegmentList).isEmpty();
        // log storage should clean up the temporary data
        assertThat(listRemoteLogFiles(tb)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitDeleteLogSegmentsFromRemoteFailed1(boolean partitionTable) throws Exception {
        long ts1 = manualClock.milliseconds();
        TableBucket tb = makeTableBucket(partitionTable);
        // Need to make leader by ReplicaManager.
        makeLogTableAsLeader(tb, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5, false);

        // should upload 4 segments to remote, only 1 active segment left
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        List<RemoteLogSegment> remoteLogSegments = remoteLog.allRemoteLogSegments();
        assertThat(remoteLogSegments)
                .hasSize(4)
                .allSatisfy(s -> assertThat(s.maxTimestamp()).isEqualTo(ts1));
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(40L);
        // check remote storage has the files
        assertThat(listRemoteLogFiles(tb))
                .isEqualTo(
                        remoteLogSegments.stream()
                                .map(s -> s.remoteLogSegmentId().toString())
                                .collect(Collectors.toSet()));

        // all upload segments should be deleted after commit.
        manualClock.advanceTime(Duration.ofDays(8));
        // make write manifest fail
        remoteLogStorage.writeManifestFail.set(true);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(remoteLog.allRemoteLogSegments()).isEqualTo(remoteLogSegments);
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(40L);
        // remote storage should not delete files
        assertThat(listRemoteLogFiles(tb))
                .isEqualTo(
                        remoteLogSegments.stream()
                                .map(s -> s.remoteLogSegmentId().toString())
                                .collect(Collectors.toSet()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitDeleteLogSegmentsFromRemoteFailed2(boolean partitionTable) throws Exception {
        long ts1 = manualClock.milliseconds();
        TableBucket tb = makeTableBucket(partitionTable);
        // Need to make leader by ReplicaManager.
        makeLogTableAsLeader(tb, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5, false);

        // should upload 4 segments to remote, only 1 active segment left
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        List<RemoteLogSegment> remoteLogSegments = remoteLog.allRemoteLogSegments();
        assertThat(remoteLogSegments)
                .hasSize(4)
                .allSatisfy(s -> assertThat(s.maxTimestamp()).isEqualTo(ts1));
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(40L);
        // check remote storage has the files
        assertThat(listRemoteLogFiles(tb))
                .isEqualTo(
                        remoteLogSegments.stream()
                                .map(s -> s.remoteLogSegmentId().toString())
                                .collect(Collectors.toSet()));

        // all upload segments should be deleted after commit.
        manualClock.advanceTime(Duration.ofDays(8));
        // mock commit fail by making CommitRemoteLogManifestRequest failed.
        testCoordinatorGateway.commitRemoteLogManifestFail.set(true);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(remoteLog.allRemoteLogSegments()).isEqualTo(remoteLogSegments);
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(40L);
        // remote storage should not delete files
        assertThat(listRemoteLogFiles(tb))
                .isEqualTo(
                        remoteLogSegments.stream()
                                .map(s -> s.remoteLogSegmentId().toString())
                                .collect(Collectors.toSet()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFetchRecordsFromRemote(boolean partitionTable) throws Exception {
        TableBucket tb = makeTableBucket(partitionTable);
        // Need to make leader by ReplicaManager.
        makeLogTableAsLeader(tb, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);
        // trigger RLMTask copy local log segment to remote and update metadata.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        List<RemoteLogSegment> remoteLogSegmentList =
                remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(remoteLogSegmentList.size()).isEqualTo(4);
        assertThat(remoteLogManager.lookupPositionForOffset(remoteLogSegmentList.get(0), 2L))
                .isGreaterThan(10);

        // 1. first, fetch records from remote.
        // mock to update remote log end offset and delete local log segments.
        logTablet.updateRemoteLogEndOffset(40L);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(-1, Integer.MAX_VALUE),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 0L, 1024 * 1024)),
                null,
                future::complete);
        Map<TableBucket, FetchLogResultForBucket> result = future.get();
        assertThat(result.size()).isEqualTo(1);
        FetchLogResultForBucket resultForBucket = result.get(tb);
        assertThat(resultForBucket.getError()).isEqualTo(ApiError.NONE);
        assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
        assertThat(resultForBucket.fetchFromRemote()).isTrue();
        assertThat(resultForBucket.records()).isNull();
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(50L);

        RemoteLogFetchInfo remoteLogFetchInfo = resultForBucket.remoteLogFetchInfo();
        assertThat(remoteLogFetchInfo).isNotNull();
        assertThat(remoteLogFetchInfo.remoteLogSegmentList().size()).isEqualTo(4);

        // 2. then, fetch records from active segments, result rhe local records in server.
        future = new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(-1, Integer.MAX_VALUE),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 47, 1024 * 1024)),
                null,
                future::complete);
        result = future.get();
        assertThat(result.size()).isEqualTo(1);
        resultForBucket = result.get(tb);
        assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(50L);
        assertThat(resultForBucket.records()).isNotNull();
        assertThat(resultForBucket.fetchFromRemote()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCleanupLocalSegments(boolean partitionTable) throws Exception {
        TableBucket tb = makeTableBucket(partitionTable);
        // Need to make leader by ReplicaManager.
        makeKvTableAsLeader(tb, DATA1_TABLE_PATH_PK, INITIAL_LEADER_EPOCH, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();

        // 2. generate 5 segments and trigger upload 4 to remote storage
        addMultiSegmentsToLogTablet(logTablet, 5);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        assertThat(remoteLog.allRemoteLogSegments()).hasSize(4);

        // 3. mock to update remote end offset, shouldn't cleanup local segments
        logTablet.updateRemoteLogEndOffset(40L);
        assertThat(logTablet.getSegments()).hasSize(5);

        // 4. mock to update min retain, should remove the first 3 segments (end offset < 33)
        logTablet.updateMinRetainOffset(33);
        assertThat(logTablet.getSegments()).hasSize(2);

        // 5. should fetch from remote, because local doesn't have the data
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(-1, Integer.MAX_VALUE),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 0L, 1024 * 1024)),
                null,
                future::complete);
        Map<TableBucket, FetchLogResultForBucket> result = future.get();
        assertThat(result.size()).isEqualTo(1);
        FetchLogResultForBucket resultForBucket = result.get(tb);
        assertThat(resultForBucket.getError()).isEqualTo(ApiError.NONE);
        assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
        assertThat(resultForBucket.fetchFromRemote()).isTrue();
        assertThat(resultForBucket.records()).isNull();
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(50L);

        RemoteLogFetchInfo remoteLogFetchInfo = resultForBucket.remoteLogFetchInfo();
        assertThat(remoteLogFetchInfo).isNotNull();
        assertThat(remoteLogFetchInfo.remoteLogSegmentList().size()).isEqualTo(4);

        // 6. then, fetch records from active segments, result rhe local records in server.
        future = new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(-1, Integer.MAX_VALUE),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 35, 1024 * 1024)),
                null,
                future::complete);
        result = future.get();
        assertThat(result.size()).isEqualTo(1);
        resultForBucket = result.get(tb);
        assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(50L);
        assertThat(resultForBucket.records()).isNotNull();
        assertThat(resultForBucket.fetchFromRemote()).isFalse();

        // 7. mock to update min retain to be same to remote end offset,
        //  although 4 segments has been updated to remote, should still retain 2 segments in local
        // since 2(by default) segments is configured to retain in local
        logTablet.updateMinRetainOffset(40L);
        assertThat(logTablet.getSegments()).hasSize(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testConfigureTieredLogLocalSegments(boolean partitionedTable) throws Exception {
        int tieredLogLocalSegments = 8;
        long tableId =
                registerTableInZkClient(
                        DATA1_TABLE_PATH,
                        DATA1_SCHEMA,
                        200L,
                        Collections.emptyList(),
                        Collections.singletonMap(
                                ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS.key(),
                                String.valueOf(tieredLogLocalSegments)));
        TableBucket tb = makeTableBucket(tableId, partitionedTable);

        // make leader, and then remote log tablet should be created.
        makeLogTableAsLeader(tb, partitionedTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();
        // verify upload log segment to remote.
        addMultiSegmentsToLogTablet(replicaManager.getReplicaOrException(tb).getLogTablet(), 10);
        // trigger RLMTask copy local log segment to remote and update metadata.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        List<RemoteLogSegment> remoteLogSegmentList =
                remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        // should upload 9 segments except for one active segment to remote
        assertThat(remoteLogSegmentList).hasSize(9);
        //  should still retain 8 segments since 8 segments is configured to retain in local
        assertThat(logTablet.getSegments()).hasSize(tieredLogLocalSegments);

        // fetch from offset 20 should be from local
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(-1, Integer.MAX_VALUE),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 20L, 1024 * 1024)),
                null,
                future::complete);
        Map<TableBucket, FetchLogResultForBucket> result = future.get();
        assertThat(result.get(tb).fetchFromRemote()).isFalse();
        assertThat(result.get(tb).records()).isNotNull();

        // fetch from offset 0, should be from remote
        future = new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(-1, Integer.MAX_VALUE),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 0, 1024 * 1024)),
                null,
                future::complete);
        result = future.get();
        assertThat(result.get(tb).fetchFromRemote()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testStopReplica(boolean partitionTable) throws Exception {
        TableBucket tb = makeTableBucket(partitionTable);
        // Need to make leader by ReplicaManager.
        makeLogTableAsLeader(tb, partitionTable);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);
        // trigger RLMTask copy local log segment to remote and update metadata.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        List<RemoteLogSegment> remoteLogSegmentList =
                remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(remoteLogSegmentList.size()).isEqualTo(4);
        assertThat(remoteLogManager.lookupPositionForOffset(remoteLogSegmentList.get(0), 2L))
                .isGreaterThan(10);
        assertThat(listRemoteLogFiles(tb))
                .isEqualTo(
                        remoteLogSegmentList.stream()
                                .map(s -> s.remoteLogSegmentId().toString())
                                .collect(Collectors.toSet()));

        // test stop replica and delete.
        remoteLogManager.stopReplica(replica, true);
        assertThatThrownBy(() -> remoteLogManager.relevantRemoteLogSegments(tb, 0L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RemoteLogTablet can't be found for table-bucket " + tb);
        FsPath logTabletDir = remoteLogTabletDir(remoteLogDir(conf), DATA1_PHYSICAL_TABLE_PATH, tb);
        assertThat(logTabletDir.getFileSystem().exists(logTabletDir)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLookupOffsetForTimestamp(boolean partitionTable) throws Exception {
        TableBucket tb = makeTableBucket(partitionTable);
        // Need to make leader by ReplicaManager.
        makeLogTableAsLeader(tb, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();
        long startTimestamp = manualClock.milliseconds();
        addMultiSegmentsToLogTablet(logTablet, 5);
        // trigger RLMTask copy local log segment to remote and update metadata.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        assertThat(remoteLog.allRemoteLogSegments()).hasSize(4);

        assertThat(remoteLogManager.lookupOffsetForTimestamp(tb, startTimestamp)).isEqualTo(0L);
        remoteLogManager
                .relevantRemoteLogSegments(tb, 0L)
                .forEach(
                        remoteLogSegment ->
                                assertThat(
                                                remoteLogManager.lookupOffsetForTimestamp(
                                                        tb, remoteLogSegment.maxTimestamp()))
                                        .isLessThan(remoteLogSegment.remoteLogEndOffset())
                                        .isGreaterThan(remoteLogSegment.remoteLogStartOffset()));
        assertThat(remoteLogManager.lookupOffsetForTimestamp(tb, startTimestamp + 5000))
                .isEqualTo(-1L);
    }

    private TableBucket makeTableBucket(boolean partitionTable) {
        return makeTableBucket(DATA1_TABLE_ID, partitionTable);
    }

    private TableBucket makeTableBucket(long tableId, boolean partitionTable) {
        if (partitionTable) {
            return new TableBucket(tableId, 0L, 0);
        } else {
            return new TableBucket(tableId, 0);
        }
    }
}
