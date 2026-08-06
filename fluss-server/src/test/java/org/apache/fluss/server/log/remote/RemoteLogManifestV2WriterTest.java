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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.remote.RemoteLogSegmentReference;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for the feature-gated Manifest V2 writer. */
class RemoteLogManifestV2WriterTest extends RemoteLogTestBase {

    @Override
    public Configuration getServerConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.LOG_INDEX_INTERVAL_SIZE, MemorySize.parse("1b"));
        conf.set(ConfigOptions.REMOTE_LOG_INDEX_FILE_CACHE_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.REMOTE_FS_WRITE_BUFFER_SIZE, MemorySize.parse("10b"));
        conf.setInt(ConfigOptions.REMOTE_LOG_TASK_MAX_UPLOAD_SEGMENTS, Integer.MAX_VALUE);
        conf.setBoolean(ConfigOptions.REMOTE_LOG_MANIFEST_V2_WRITER_ENABLED, true);
        conf.setBoolean(ConfigOptions.REMOTE_LOG_MANIFEST_V2_GC_ENABLED, true);
        conf.set(ConfigOptions.REMOTE_LOG_MANIFEST_V2_GC_GRACE_PERIOD, Duration.ofHours(1));
        return conf;
    }

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
    }

    @Test
    void testInitialCopyPublishesOneGenerationForMultipleSegments() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        addMultiSegmentsToLogTablet(
                replicaManager.getReplicaOrException(tableBucket).getLogTablet(), 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogTablet remoteLogTablet = remoteLogManager.remoteLogTablet(tableBucket);
        assertThat(remoteLogTablet.currentManifest().getVersion())
                .isEqualTo(RemoteLogManifest.VERSION_2);
        assertThat(remoteLogTablet.currentManifest().getGeneration()).isEqualTo(1L);
        assertThat(remoteLogTablet.allRemoteLogSegments()).hasSize(4);
        assertThat(remoteLogTablet.currentHandle()).isNotNull();
        assertThat(remoteLogTablet.currentHandle().handle().getVersion())
                .isEqualTo(RemoteLogManifestHandle.VERSION_2);
    }

    @Test
    void testOverlappingLocalSegmentReplacesRemoteSuffix() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        RemoteLogSegment remoteSegment =
                RemoteLogSegment.Builder.builder()
                        .physicalTablePath(DATA1_PHYSICAL_TABLE_PATH)
                        .tableBucket(tableBucket)
                        .remoteLogSegmentId(UUID.randomUUID())
                        .remoteLogStartOffset(0L)
                        .remoteLogEndOffset(15L)
                        .maxTimestamp(manualClock.milliseconds())
                        .segmentSizeInBytes(1)
                        .build();
        RemoteLogManifest baseManifest =
                new RemoteLogManifest(
                        DATA1_PHYSICAL_TABLE_PATH,
                        tableBucket,
                        Collections.singletonList(remoteSegment));
        FsPath basePath = remoteLogStorage.writeRemoteLogManifestSnapshot(baseManifest);
        zkClient.createRemoteLogManifestHandleIfAbsent(
                tableBucket, new RemoteLogManifestHandle(basePath, 15L));

        makeLogTableAsLeader(tableBucket, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tableBucket).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 3);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest result = remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(result.getGeneration()).isEqualTo(1L);
        assertThat(result.getRemoteLogSegmentList()).hasSize(2);
        RemoteLogSegment replacement = result.getRemoteLogSegmentList().get(1);
        assertThat(replacement.remoteLogStartOffset()).isEqualTo(10L);
        assertThat(replacement.remoteLogEndOffset()).isEqualTo(20L);
        assertThat(result.getRemoteLogSegmentReferences())
                .containsExactly(
                        new RemoteLogSegmentReference(remoteSegment, 0L, 10L),
                        new RemoteLogSegmentReference(replacement, 10L, 20L));
    }

    @Test
    void testPartialCopyCommitsOnlySuccessfulPrefix() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        addMultiSegmentsToLogTablet(
                replicaManager.getReplicaOrException(tableBucket).getLogTablet(), 5);
        remoteLogStorage.copySegmentFailAfterNCopies.set(2);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest first = remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(first.getGeneration()).isEqualTo(1L);
        assertThat(first.getRemoteLogSegmentList()).hasSize(2);
        assertThat(first.getRemoteLogEndOffset()).isEqualTo(20L);

        remoteLogStorage.copySegmentFailAfterNCopies.set(-1);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest second = remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(second.getGeneration()).isEqualTo(2L);
        assertThat(second.getRemoteLogSegmentList()).hasSize(4);
        assertThat(second.getRemoteLogEndOffset()).isEqualTo(40L);
    }

    @Test
    void testCasConflictCleansRejectedObjectsAndReplans() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        addMultiSegmentsToLogTablet(
                replicaManager.getReplicaOrException(tableBucket).getLogTablet(), 3);
        testCoordinatorGateway.commitRemoteLogManifestConflictOnce.set(true);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest result = remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(result.getGeneration()).isEqualTo(1L);
        assertThat(result.getRemoteLogSegmentList()).hasSize(2);
        assertThat(listRemoteLogFiles(tableBucket))
                .isEqualTo(
                        result.getRemoteLogSegmentList().stream()
                                .map(segment -> segment.remoteLogSegmentId().toString())
                                .collect(Collectors.toSet()));
    }

    @Test
    void testTtlMovesPrefixToUnreferencedWithoutPhysicalDeletion() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tableBucket).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5, false);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        List<RemoteLogSegment> firstGenerationSegments =
                remoteLogManager
                        .remoteLogTablet(tableBucket)
                        .currentManifest()
                        .getRemoteLogSegmentList();

        manualClock.advanceTime(Duration.ofDays(8));
        addMultiSegmentsToLogTablet(logTablet, 2, false);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest result = remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(result.getUnreferencedRemoteLogSegments()).isNotEmpty();
        assertThat(result.getUnreferencedRemoteLogSegments())
                .extracting(entry -> entry.remoteLogSegment().remoteLogSegmentId())
                .containsAll(
                        firstGenerationSegments.stream()
                                .map(RemoteLogSegment::remoteLogSegmentId)
                                .collect(Collectors.toSet()));
        assertThat(listRemoteLogFiles(tableBucket))
                .containsAll(
                        result.getUnreferencedRemoteLogSegments().stream()
                                .map(
                                        entry ->
                                                entry.remoteLogSegment()
                                                        .remoteLogSegmentId()
                                                        .toString())
                                .collect(Collectors.toSet()));
    }

    @Test
    void testGcDeletesAfterGraceAndThenRemovesMetadata() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tableBucket).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5, false);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        manualClock.advanceTime(Duration.ofDays(8));
        addMultiSegmentsToLogTablet(logTablet, 2, false);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogManifest beforeGc =
                remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(beforeGc.getUnreferencedRemoteLogSegments()).isNotEmpty();
        assertThat(remoteLogManager.remoteLogTablet(tableBucket).getUnreferencedSegmentCount())
                .isEqualTo(beforeGc.getUnreferencedRemoteLogSegments().size());
        assertThat(remoteLogManager.remoteLogTablet(tableBucket).getUnreferencedSizeInBytes())
                .isPositive();
        List<String> garbageIds =
                beforeGc.getUnreferencedRemoteLogSegments().stream()
                        .map(entry -> entry.remoteLogSegment().remoteLogSegmentId().toString())
                        .collect(Collectors.toList());

        manualClock.advanceTime(Duration.ofHours(1));
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest afterGc = remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(afterGc.getGeneration()).isEqualTo(beforeGc.getGeneration() + 1L);
        assertThat(afterGc.getUnreferencedRemoteLogSegments()).isEmpty();
        assertThat(remoteLogManager.remoteLogTablet(tableBucket).getUnreferencedSizeInBytes())
                .isZero();
        assertThat(listRemoteLogFiles(tableBucket)).doesNotContainAnyElementsOf(garbageIds);
    }

    @Test
    void testGcDeleteFailureRetainsMetadataAndRetries() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tableBucket).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5, false);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        manualClock.advanceTime(Duration.ofDays(8));
        addMultiSegmentsToLogTablet(logTablet, 2, false);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogManifest beforeGc =
                remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        int unreferencedCount = beforeGc.getUnreferencedRemoteLogSegments().size();

        manualClock.advanceTime(Duration.ofHours(1));
        remoteLogStorage.deleteSegmentFailFirstN.set(unreferencedCount);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(
                        remoteLogManager
                                .remoteLogTablet(tableBucket)
                                .currentManifest()
                                .getUnreferencedRemoteLogSegments())
                .hasSize(unreferencedCount);
        assertThat(
                        replicaManager
                                .getReplicaOrException(tableBucket)
                                .tableMetrics()
                                .remoteGcFailures()
                                .getCount())
                .isEqualTo(unreferencedCount);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(
                        remoteLogManager
                                .remoteLogTablet(tableBucket)
                                .currentManifest()
                                .getUnreferencedRemoteLogSegments())
                .isEmpty();
    }

    @Test
    void testOrphanSweeperCleansRejectedCopyAfterGrace() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        addMultiSegmentsToLogTablet(
                replicaManager.getReplicaOrException(tableBucket).getLogTablet(), 3);
        testCoordinatorGateway.commitRemoteLogManifestConflictOnce.set(true);
        remoteLogStorage.deleteSegmentFailFirstN.set(2);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogManifest authoritative =
                remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(listRemoteLogFiles(tableBucket).size())
                .isGreaterThan(authoritative.getRemoteLogSegmentList().size());

        manualClock.advanceTime(Duration.ofHours(1).plusSeconds(1));
        remoteLogStorage.deleteSegmentObjectFailFirstN.set(1);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(listRemoteLogFiles(tableBucket).size())
                .isGreaterThan(authoritative.getRemoteLogSegmentList().size());

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(listRemoteLogFiles(tableBucket))
                .isEqualTo(
                        authoritative.getRemoteLogSegmentList().stream()
                                .map(segment -> segment.remoteLogSegmentId().toString())
                                .collect(Collectors.toSet()));
    }

    @Test
    void testOrphanManifestSweeperPreservesAuthoritativeSnapshot() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        addMultiSegmentsToLogTablet(
                replicaManager.getReplicaOrException(tableBucket).getLogTablet(), 3);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogTablet remoteLogTablet = remoteLogManager.remoteLogTablet(tableBucket);
        FsPath authoritativePath =
                remoteLogTablet.currentHandle().handle().getRemoteLogManifestPath();
        FsPath orphanPath =
                remoteLogStorage.writeRemoteLogManifestSnapshot(remoteLogTablet.currentManifest());

        manualClock.advanceTime(Duration.ofHours(1).plusSeconds(1));
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        assertThat(
                        remoteLogStorage.listRemoteLogManifestSnapshots(
                                DATA1_PHYSICAL_TABLE_PATH, tableBucket))
                .extracting(object -> object.path().getPath())
                .contains(authoritativePath.getPath())
                .doesNotContain(orphanPath.getPath());
    }
}
