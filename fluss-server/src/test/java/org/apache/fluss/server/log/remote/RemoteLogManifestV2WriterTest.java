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
import org.apache.fluss.remote.UnreferencedRemoteLogSegment;
import org.apache.fluss.server.entity.RemoteLogManifestCommitResult;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for the feature-gated Manifest V2 writer. */
class RemoteLogManifestV2WriterTest extends RemoteLogTestBase {

    private final AtomicBoolean manifestV2WriterEnabled = new AtomicBoolean(true);

    @Override
    protected BooleanSupplier manifestV2WriterEnabledSupplier() {
        return manifestV2WriterEnabled::get;
    }

    @Override
    public Configuration getServerConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.LOG_INDEX_INTERVAL_SIZE, MemorySize.parse("1b"));
        conf.set(ConfigOptions.REMOTE_LOG_INDEX_FILE_CACHE_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.REMOTE_FS_WRITE_BUFFER_SIZE, MemorySize.parse("10b"));
        conf.setInt(ConfigOptions.REMOTE_LOG_TASK_MAX_UPLOAD_SEGMENTS, Integer.MAX_VALUE);
        conf.setBoolean(ConfigOptions.REMOTE_LOG_MANIFEST_V2_WRITER_ENABLED, true);
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
    void testInterruptedCopyDoesNotPublishCopiedPrefix() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        addMultiSegmentsToLogTablet(
                replicaManager.getReplicaOrException(tableBucket).getLogTablet(), 3);
        remoteLogStorage.copySegmentInterruptAfterNCopies.set(1);

        boolean interrupted;
        try {
            remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
            interrupted = Thread.currentThread().isInterrupted();
        } finally {
            Thread.interrupted();
        }

        assertThat(interrupted).isTrue();
        assertThat(remoteLogStorage.getCopySegmentCount()).isEqualTo(1);
        assertThat(remoteLogManager.remoteLogTablet(tableBucket).currentManifest().getVersion())
                .isEqualTo(RemoteLogManifest.VERSION_1);
        assertThat(zkClient.getVersionedRemoteLogManifestHandle(tableBucket)).isEmpty();
    }

    @Test
    void testExistingTieringTaskSwitchesToV2AfterDynamicActivation() throws Exception {
        manifestV2WriterEnabled.set(false);
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        addMultiSegmentsToLogTablet(
                replicaManager.getReplicaOrException(tableBucket).getLogTablet(), 3);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(remoteLogManager.remoteLogTablet(tableBucket).currentManifest().getVersion())
                .isEqualTo(RemoteLogManifest.VERSION_1);

        manifestV2WriterEnabled.set(true);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        assertThat(remoteLogManager.remoteLogTablet(tableBucket).currentManifest().getVersion())
                .isEqualTo(RemoteLogManifest.VERSION_2);
    }

    @Test
    void testDynamicActivationMigratesDuplicateV1Range() throws Exception {
        manifestV2WriterEnabled.set(false);
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        RemoteLogSegment earlier = remoteSegment(tableBucket, 0L, 10L, manualClock.milliseconds());
        RemoteLogSegment later = remoteSegment(tableBucket, 0L, 10L, manualClock.milliseconds());
        RemoteLogManifest duplicateV1 =
                new RemoteLogManifest(
                        DATA1_PHYSICAL_TABLE_PATH, tableBucket, Arrays.asList(earlier, later));
        FsPath basePath = remoteLogStorage.writeRemoteLogManifestSnapshot(duplicateV1);
        zkClient.createRemoteLogManifestHandleIfAbsent(
                tableBucket, new RemoteLogManifestHandle(basePath, 10L));
        makeLogTableAsLeader(tableBucket, false);
        addMultiSegmentsToLogTablet(
                replicaManager.getReplicaOrException(tableBucket).getLogTablet(), 2);

        manifestV2WriterEnabled.set(true);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest migrated =
                remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(migrated.getVersion()).isEqualTo(RemoteLogManifest.VERSION_2);
        assertThat(migrated.getRemoteLogSegmentList()).first().isEqualTo(later);
        assertThat(migrated.getUnreferencedRemoteLogSegments())
                .singleElement()
                .satisfies(
                        unreferenced -> {
                            assertThat(unreferenced.remoteLogSegment()).isEqualTo(earlier);
                            assertThat(unreferenced.replacementSegmentId())
                                    .isEqualTo(later.remoteLogSegmentId());
                        });
    }

    @Test
    void testV1PathDoesNotCopyWhenAuthoritativeManifestIsV2() throws Exception {
        manifestV2WriterEnabled.set(false);
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        RemoteLogManifest emptyV2 =
                RemoteLogManifest.createV2(
                        1L,
                        DATA1_PHYSICAL_TABLE_PATH,
                        tableBucket,
                        Collections.emptyList(),
                        null,
                        -1L,
                        Collections.emptyList());
        FsPath manifestPath = remoteLogStorage.writeRemoteLogManifestSnapshot(emptyV2);
        zkClient.createRemoteLogManifestHandleIfAbsent(
                tableBucket, RemoteLogManifestHandle.v2Empty(manifestPath, 1L));
        makeLogTableAsLeader(tableBucket, false);
        addMultiSegmentsToLogTablet(
                replicaManager.getReplicaOrException(tableBucket).getLogTablet(), 3);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        assertThat(remoteLogStorage.getCopySegmentCount()).isZero();
        assertThat(remoteLogManager.remoteLogTablet(tableBucket).currentManifest())
                .isEqualTo(emptyV2);
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
    void testGappedV1MigrationRebuildsFromLocalLog() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        RemoteLogSegment first = remoteSegment(tableBucket, 0L, 5L);
        RemoteLogSegment afterGap = remoteSegment(tableBucket, 15L, 20L);
        RemoteLogManifest gappedV1 =
                new RemoteLogManifest(
                        DATA1_PHYSICAL_TABLE_PATH, tableBucket, Arrays.asList(first, afterGap));
        FsPath basePath = remoteLogStorage.writeRemoteLogManifestSnapshot(gappedV1);
        zkClient.createRemoteLogManifestHandleIfAbsent(
                tableBucket, new RemoteLogManifestHandle(basePath, 20L));

        makeLogTableAsLeader(tableBucket, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tableBucket).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 3);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest result = remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(result.getVersion()).isEqualTo(RemoteLogManifest.VERSION_2);
        assertThat(result.getRemoteLogStartOffset()).isEqualTo(logTablet.localLogStartOffset());
        assertThat(result.getRemoteLogEndOffset()).isEqualTo(20L);
        assertThat(result.getRemoteLogSegmentList()).hasSize(2);
        assertThat(result.getUnreferencedRemoteLogSegments())
                .extracting(UnreferencedRemoteLogSegment::remoteLogSegment)
                .containsExactlyInAnyOrder(first, afterGap);
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

    private static RemoteLogSegment remoteSegment(
            TableBucket tableBucket, long startOffset, long endOffset) {
        return remoteSegment(tableBucket, startOffset, endOffset, 0L);
    }

    private static RemoteLogSegment remoteSegment(
            TableBucket tableBucket, long startOffset, long endOffset, long maxTimestamp) {
        return RemoteLogSegment.Builder.builder()
                .physicalTablePath(DATA1_PHYSICAL_TABLE_PATH)
                .tableBucket(tableBucket)
                .remoteLogSegmentId(UUID.randomUUID())
                .remoteLogStartOffset(startOffset)
                .remoteLogEndOffset(endOffset)
                .maxTimestamp(maxTimestamp)
                .segmentSizeInBytes(1)
                .build();
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
    void testCoordinatorV2WriterDisabledFallsBackToV1AndRetriesV2NextRun() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        addMultiSegmentsToLogTablet(
                replicaManager.getReplicaOrException(tableBucket).getLogTablet(), 3);
        testCoordinatorGateway.commitRemoteLogManifestResultOnce.set(
                RemoteLogManifestCommitResult.V2_WRITER_DISABLED);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest fallback =
                remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(fallback.getVersion()).isEqualTo(RemoteLogManifest.VERSION_1);
        assertThat(fallback.getRemoteLogSegmentList()).hasSize(2);
        assertThat(listRemoteLogFiles(tableBucket))
                .isEqualTo(
                        fallback.getRemoteLogSegmentList().stream()
                                .map(segment -> segment.remoteLogSegmentId().toString())
                                .collect(Collectors.toSet()));

        int copyCountBeforeProbe = remoteLogStorage.getCopySegmentCount();
        int segmentCountBeforeProbe = fallback.getRemoteLogSegmentList().size();
        addMultiSegmentsToLogTablet(
                replicaManager.getReplicaOrException(tableBucket).getLogTablet(), 2);
        testCoordinatorGateway.commitRemoteLogManifestResultOnce.set(
                RemoteLogManifestCommitResult.V2_WRITER_DISABLED);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest secondFallback =
                remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        int copiedByV1 = secondFallback.getRemoteLogSegmentList().size() - segmentCountBeforeProbe;
        assertThat(remoteLogStorage.getCopySegmentCount() - copyCountBeforeProbe)
                .isEqualTo(copiedByV1);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        assertThat(remoteLogManager.remoteLogTablet(tableBucket).currentManifest().getVersion())
                .isEqualTo(RemoteLogManifest.VERSION_2);
    }

    @ParameterizedTest
    @EnumSource(
            value = RemoteLogManifestCommitResult.class,
            names = {"INVALID_MANIFEST", "FENCED"})
    void testHardV2RejectionCleansPlanWithoutV1Fallback(RemoteLogManifestCommitResult commitResult)
            throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        addMultiSegmentsToLogTablet(
                replicaManager.getReplicaOrException(tableBucket).getLogTablet(), 3);
        testCoordinatorGateway.commitRemoteLogManifestResultOnce.set(commitResult);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest result = remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(result.getVersion()).isEqualTo(RemoteLogManifest.VERSION_1);
        assertThat(result.getRemoteLogSegmentList()).isEmpty();
        assertThat(listRemoteLogFiles(tableBucket)).isEmpty();
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
                .allMatch(entry -> !entry.isGcEligible());
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
    void testTtlPublishesEmptyManifestForIdleBucket() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tableBucket).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 2, false);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest beforeExpiration =
                remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(beforeExpiration.getRemoteLogSegmentList()).hasSize(1);
        long highestCopiedEndOffset = beforeExpiration.getHighestCopiedEndOffset();

        manualClock.advanceTime(Duration.ofDays(8));
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogTablet remoteLogTablet = remoteLogManager.remoteLogTablet(tableBucket);
        RemoteLogManifest emptyManifest = remoteLogTablet.currentManifest();
        assertThat(emptyManifest.getRemoteLogSegmentList()).isEmpty();
        assertThat(emptyManifest.getRemoteLogStartOffset()).isEqualTo(Long.MAX_VALUE);
        assertThat(emptyManifest.getRemoteLogEndOffset()).isEqualTo(-1L);
        assertThat(emptyManifest.getHighestCopiedEndOffset()).isEqualTo(highestCopiedEndOffset);
        assertThat(emptyManifest.getUnreferencedRemoteLogSegments()).hasSize(1);
        assertThat(remoteLogTablet.currentHandle()).isNotNull();
        assertThat(remoteLogTablet.currentHandle().handle().isEmptyV2()).isTrue();
        assertThat(emptyManifest.getUnreferencedRemoteLogSegments())
                .allMatch(entry -> !entry.isGcEligible());

        manualClock.advanceTime(Duration.ofHours(1));
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest eligibleManifest = remoteLogTablet.currentManifest();
        assertThat(eligibleManifest.getUnreferencedRemoteLogSegments())
                .allMatch(UnreferencedRemoteLogSegment::isGcEligible);
        assertThat(eligibleManifest.getHighestCopiedEndOffset()).isEqualTo(highestCopiedEndOffset);
        assertThat(listRemoteLogFiles(tableBucket)).isNotEmpty();

        manualClock.advanceTime(Duration.ofHours(1));
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest afterGc = remoteLogTablet.currentManifest();
        assertThat(afterGc.getRemoteLogSegmentList()).isEmpty();
        assertThat(afterGc.getUnreferencedRemoteLogSegments()).isEmpty();
        assertThat(afterGc.getHighestCopiedEndOffset()).isEqualTo(highestCopiedEndOffset);
        assertThat(listRemoteLogFiles(tableBucket)).isEmpty();

        addMultiSegmentsToLogTablet(logTablet, 2, false);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest afterNewWrites = remoteLogTablet.currentManifest();
        assertThat(afterNewWrites.getRemoteLogSegmentList()).isNotEmpty();
        assertThat(afterNewWrites.getRemoteLogStartOffset()).isNotEqualTo(Long.MAX_VALUE);
        assertThat(remoteLogTablet.currentHandle().handle().isEmptyV2()).isFalse();
    }

    @Test
    void testAlreadyExpiredRolledSegmentAdvancesPersistentHighestCopiedEndOffset()
            throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tableBucket).getLogTablet();
        logTablet.updateTieredLogLocalSegments(1);
        addMultiSegmentsToLogTablet(logTablet, 1, false);

        manualClock.advanceTime(Duration.ofDays(8));
        logTablet.roll(Optional.empty());
        assertThat(logTablet.getSegments()).hasSize(2);
        assertThat(logTablet.getHighWatermark()).isEqualTo(10L);
        assertThat(logTablet.logSegments(0L, Long.MAX_VALUE)).hasSize(2);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        assertThat(remoteLogStorage.getCopySegmentCount()).isEqualTo(1);
        RemoteLogTablet remoteLogTablet = remoteLogManager.remoteLogTablet(tableBucket);
        RemoteLogManifest manifest = remoteLogTablet.currentManifest();
        assertThat(manifest.getRemoteLogSegmentList()).isEmpty();
        assertThat(manifest.getUnreferencedRemoteLogSegments()).hasSize(1);
        assertThat(manifest.getRemoteLogEndOffset()).isEqualTo(-1L);
        assertThat(manifest.getHighestCopiedEndOffset()).isEqualTo(10L);
        assertThat(remoteLogTablet.currentHandle().handle().getHighestCopiedEndOffset())
                .isEqualTo(10L);
        assertThat(logTablet.getSegments()).hasSize(1);
        assertThat(logTablet.activeLogSegment().getBaseOffset()).isEqualTo(10L);
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
        assertThat(beforeGc.getUnreferencedRemoteLogSegments())
                .allMatch(entry -> !entry.isGcEligible());
        assertThat(remoteLogManager.remoteLogTablet(tableBucket).getUnreferencedSegmentCount())
                .isEqualTo(beforeGc.getUnreferencedRemoteLogSegments().size());
        assertThat(remoteLogManager.remoteLogTablet(tableBucket).getUnreferencedSizeInBytes())
                .isPositive();
        List<String> garbageIds =
                beforeGc.getUnreferencedRemoteLogSegments().stream()
                        .map(entry -> entry.remoteLogSegment().remoteLogSegmentId().toString())
                        .collect(Collectors.toList());

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest eligibleManifest =
                remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(eligibleManifest.getUnreferencedRemoteLogSegments())
                .allMatch(UnreferencedRemoteLogSegment::isGcEligible);
        assertThat(listRemoteLogFiles(tableBucket)).containsAll(garbageIds);

        manualClock.advanceTime(Duration.ofHours(1));
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogManifest afterGc = remoteLogManager.remoteLogTablet(tableBucket).currentManifest();
        assertThat(afterGc.getGeneration()).isEqualTo(beforeGc.getGeneration() + 2L);
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

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
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
}
