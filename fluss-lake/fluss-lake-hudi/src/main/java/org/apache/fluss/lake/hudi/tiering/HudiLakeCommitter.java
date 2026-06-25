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

package org.apache.fluss.lake.hudi.tiering;

import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.hudi.utils.meta.CkpMetadata;
import org.apache.fluss.lake.hudi.utils.meta.CkpMetadataProvider;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.util.CompactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.lake.writer.LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;

/** Hudi implementation of {@link LakeCommitter}. */
public class HudiLakeCommitter implements LakeCommitter<HudiWriteResult, HudiCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(HudiLakeCommitter.class);

    private static final String COMMITTER_USER = "commit-user";

    private final HudiWriteTableInfo hudiTableInfo;
    private final HoodieFlinkWriteClient<?> writeClient;
    private final CkpMetadata ckpMetadata;
    private final HudiCompactionService compactionService;

    public HudiLakeCommitter(
            HudiCatalogProvider hudiCatalogProvider,
            CkpMetadataProvider ckpMetadataProvider,
            TablePath tablePath)
            throws IOException {
        this.hudiTableInfo = HudiWriteTableInfo.create(hudiCatalogProvider, tablePath);
        this.writeClient = hudiTableInfo.getWriteClient();
        this.ckpMetadata = ckpMetadataProvider.get(tablePath, hudiTableInfo);
        this.compactionService = HudiCompactionService.forScheduler(hudiTableInfo);
        LOG.info(
                "Created HudiLakeCommitter with configuration {}.", hudiTableInfo.getFlinkConfig());
    }

    HudiLakeCommitter(
            HudiWriteTableInfo hudiTableInfo,
            CkpMetadata ckpMetadata,
            HudiCompactionService compactionService) {
        this.hudiTableInfo = hudiTableInfo;
        this.writeClient = hudiTableInfo.getWriteClient();
        this.ckpMetadata = ckpMetadata;
        this.compactionService = compactionService;
    }

    @Override
    public HudiCommittable toCommittable(
            List<HudiWriteResult> hudiWriteResults, @Nullable Long watermark) {
        HudiCommittable.Builder committableBuilder = HudiCommittable.builder();
        for (HudiWriteResult hudiWriteResult : hudiWriteResults) {
            committableBuilder.addWriteStats(hudiWriteResult.getWriteStats());
            committableBuilder.addCompactionWriteStats(hudiWriteResult.getCompactionWriteStats());
        }
        return committableBuilder.build();
    }

    @Override
    public LakeCommitResult commit(
            HudiCommittable committable, Map<String, String> snapshotProperties)
            throws IOException {
        Map<String, HudiWriteStats> writeStatsByInstant = committable.getWriteStats();
        if (writeStatsByInstant.size() != 1) {
            throw new IOException(
                    "Hudi write stats must contain exactly one instant, but got "
                            + writeStatsByInstant.keySet()
                            + ".");
        }

        Map.Entry<String, HudiWriteStats> entry = writeStatsByInstant.entrySet().iterator().next();
        String instant = entry.getKey();
        HudiWriteStats writeStats = entry.getValue();

        Map<String, String> commitMetadata = new HashMap<>(snapshotProperties);
        commitMetadata.put(COMMITTER_USER, FLUSS_LAKE_TIERING_COMMIT_USER);

        try {
            validateWriteStats(instant, writeStats);

            LOG.info(
                    "Committing Hudi instant {} with {} write stat entries and metadata {}.",
                    instant,
                    writeStats.getWriteStats().size(),
                    commitMetadata);
            boolean committed =
                    writeClient.commitStats(
                            instant,
                            writeStats.getWriteStats(),
                            Option.of(commitMetadata),
                            hudiTableInfo.getMetaClient().getCommitActionType());
            if (!committed) {
                IOException failure =
                        new IOException("Failed to commit Hudi instant " + instant + ".");
                try {
                    abortInstant(instant);
                } catch (IOException abortFailure) {
                    failure.addSuppressed(abortFailure);
                }
                throw failure;
            }

            ckpMetadata.commitInstant(instant);
            LOG.info("Committed Hudi instant {} successfully.", instant);
            commitCompactionAndSchedule(committable.getCompactionWriteStats());
            return LakeCommitResult.committedIsReadable(parseSnapshotId(instant));
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("Failed to commit Hudi instant " + instant + ".", e);
        }
    }

    @Override
    public void abort(HudiCommittable committable) throws IOException {
        IOException failure = null;
        for (String instant : committable.getCompactionWriteStats().keySet()) {
            try {
                abortCompactionInstant(instant);
                LOG.info("Aborted Hudi compaction instant {}.", instant);
            } catch (IOException e) {
                failure =
                        addSuppressed(
                                failure,
                                new IOException(
                                        "Failed to abort Hudi compaction instant " + instant + ".",
                                        e));
            }
        }
        for (String instant : committable.getWriteStats().keySet()) {
            try {
                abortInstant(instant);
                LOG.info("Aborted Hudi instant {}.", instant);
            } catch (IOException e) {
                failure =
                        addSuppressed(
                                failure,
                                new IOException(
                                        "Failed to abort Hudi instant " + instant + ".", e));
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        HoodieTimeline latestLakeTimeline =
                getCompletedTimelineCommittedBy(FLUSS_LAKE_TIERING_COMMIT_USER);
        Optional<HoodieInstant> latestLakeInstant =
                latestLakeTimeline.getReverseOrderedInstants().findFirst();
        if (!latestLakeInstant.isPresent()) {
            return null;
        }

        long latestLakeSnapshotId = parseSnapshotId(latestLakeInstant.get().requestedTime());
        if (latestLakeSnapshotIdOfFluss != null
                && latestLakeSnapshotId <= latestLakeSnapshotIdOfFluss) {
            return null;
        }

        HoodieCommitMetadata metadata =
                latestLakeTimeline.readCommitMetadata(latestLakeInstant.get());
        if (metadata == null) {
            throw new IOException("Failed to load committed Hudi instant metadata.");
        }
        Map<String, String> extraMetadata = metadata.getExtraMetadata();
        if (extraMetadata == null) {
            throw new IOException("Failed to load committed Hudi instant extra metadata.");
        }
        return new CommittedLakeSnapshot(latestLakeSnapshotId, extraMetadata);
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(ckpMetadata, "hudi checkpoint metadata");
        IOUtils.closeQuietly(hudiTableInfo, "hudi table info");
    }

    private void commitCompactionAndSchedule(Map<String, HudiWriteStats> compactionWriteStats)
            throws IOException {
        if (!isAutoCompactionEnabled()) {
            return;
        }

        String latestCommittedInstant =
                compactionService.commitCompaction(compactionWriteStats, Collections.emptyMap());
        if (latestCommittedInstant != null) {
            LOG.info("Committed Hudi compaction instant {}.", latestCommittedInstant);
        }

        try {
            compactionService.scheduleCompaction();
            compactionService.markSelectedCompactionsInflight();
        } catch (Exception e) {
            LOG.warn(
                    "Failed to schedule Hudi compaction for table {}.",
                    hudiTableInfo.getTablePath(),
                    e);
        }
    }

    private boolean isAutoCompactionEnabled() {
        return hudiTableInfo.getTableType() == HoodieTableType.MERGE_ON_READ
                && hudiTableInfo.getFlinkConfig().get(FlinkOptions.COMPACTION_SCHEDULE_ENABLED);
    }

    private void validateWriteStats(String instant, HudiWriteStats writeStats) {
        long totalErrorRecords = writeStats.getTotalErrorRecords();
        if (totalErrorRecords > 0
                && !hudiTableInfo.getFlinkConfig().get(FlinkOptions.IGNORE_FAILED)) {
            throw new HoodieException(
                    String.format(
                            "Commit Hudi instant %s failed with %s error records.",
                            instant, totalErrorRecords));
        }
    }

    private void abortInstant(String instant) throws IOException {
        IOException failure = null;
        try {
            boolean rolledBack = writeClient.rollback(instant);
            if (!rolledBack) {
                throw new IOException("Hudi rollback returned false for instant " + instant + ".");
            }
        } catch (Exception e) {
            failure =
                    addSuppressed(
                            failure,
                            new IOException("Failed to rollback Hudi instant " + instant + ".", e));
        }

        try {
            ckpMetadata.abortInstant(instant);
        } catch (Exception e) {
            failure =
                    addSuppressed(
                            failure,
                            new IOException(
                                    "Failed to abort Hudi checkpoint metadata for instant "
                                            + instant
                                            + ".",
                                    e));
        }

        if (failure != null) {
            throw failure;
        }
    }

    private void abortCompactionInstant(String instant) throws IOException {
        try {
            hudiTableInfo.getMetaClient().reloadActiveTimeline();
            CompactionUtil.rollbackCompaction(
                    writeClient.getHoodieTable(), instant, writeClient.getTransactionManager());
        } catch (Exception e) {
            throw new IOException("Failed to rollback Hudi compaction instant " + instant + ".", e);
        }
    }

    private HoodieTimeline getCompletedTimelineCommittedBy(String commitUser) throws IOException {
        hudiTableInfo.getMetaClient().reloadActiveTimeline();
        HoodieTimeline timeline =
                writeClient
                        .getHoodieTable()
                        .getMetaClient()
                        .getActiveTimeline()
                        .getCommitsAndCompactionTimeline()
                        .filterCompletedInstants();
        try {
            return timeline.filter(instant -> isCommittedBy(timeline, instant, commitUser));
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static boolean isCommittedBy(
            HoodieTimeline timeline, HoodieInstant instant, String commitUser) {
        try {
            HoodieCommitMetadata metadata = timeline.readCommitMetadata(instant);
            if (metadata == null) {
                throw new IOException("Failed to load committed Hudi instant metadata.");
            }
            Map<String, String> extraMetadata = metadata.getExtraMetadata();
            return metadata.getOperationType() != WriteOperationType.COMPACT
                    && extraMetadata != null
                    && commitUser.equals(extraMetadata.get(COMMITTER_USER));
        } catch (IOException e) {
            // a read failure must not be silently treated as "not committed by Fluss",
            // otherwise we may miss an already-tiered snapshot and re-commit duplicated data
            throw new UncheckedIOException(
                    "Failed to read Hudi commit metadata for instant " + instant + ".", e);
        }
    }

    private static long parseSnapshotId(String instant) throws IOException {
        try {
            return Long.parseLong(instant);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid Hudi instant time " + instant + ".", e);
        }
    }

    private static IOException addSuppressed(IOException failure, IOException current) {
        if (failure == null) {
            return current;
        }
        failure.addSuppressed(current);
        return failure;
    }
}
