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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.lake.writer.LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;

/** Hudi implementation of {@link LakeCommitter}. */
public class HudiLakeCommitter implements LakeCommitter<HudiWriteResult, HudiCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(HudiLakeCommitter.class);

    private static final String COMMITTER_USER = "commit-user";

    private final HudiWriteTableInfo hudiTableInfo;
    private final HoodieFlinkWriteClient<?> writeClient;
    private final CkpMetadata ckpMetadata;

    public HudiLakeCommitter(
            HudiCatalogProvider hudiCatalogProvider,
            CkpMetadataProvider ckpMetadataProvider,
            TablePath tablePath)
            throws IOException {
        this.hudiTableInfo = HudiWriteTableInfo.create(hudiCatalogProvider, tablePath);
        this.writeClient = hudiTableInfo.getWriteClient();
        this.ckpMetadata = ckpMetadataProvider.get(tablePath, hudiTableInfo);
        LOG.info(
                "Created HudiLakeCommitter with configuration {}.", hudiTableInfo.getFlinkConfig());
    }

    @Override
    public HudiCommittable toCommittable(List<HudiWriteResult> hudiWriteResults) {
        HudiCommittable.Builder committableBuilder = HudiCommittable.builder();
        for (HudiWriteResult hudiWriteResult : hudiWriteResults) {
            committableBuilder.addWriteStatuses(hudiWriteResult.getWriteStatuses());
            committableBuilder.addCompactionWriteStatuses(
                    hudiWriteResult.getCompactionWriteStatuses());
        }
        return committableBuilder.build();
    }

    @Override
    public LakeCommitResult commit(
            HudiCommittable committable, Map<String, String> snapshotProperties)
            throws IOException {
        ensureNoCompactionWriteStatuses(committable);

        Map<String, List<WriteStatus>> writeStatuses = committable.getWriteStatuses();
        if (writeStatuses.size() != 1) {
            throw new IOException(
                    "Hudi write statuses must contain exactly one instant, but got "
                            + writeStatuses.keySet()
                            + ".");
        }

        Map.Entry<String, List<WriteStatus>> entry = writeStatuses.entrySet().iterator().next();
        String instant = entry.getKey();
        List<WriteStatus> statuses = entry.getValue();

        Map<String, String> commitMetadata = new HashMap<>(snapshotProperties);
        commitMetadata.put(COMMITTER_USER, FLUSS_LAKE_TIERING_COMMIT_USER);

        try {
            validateWriteStatuses(instant, statuses);
            if (writeClient.getHeartbeatClient().isHeartbeatExpired(instant)) {
                writeClient.getHeartbeatClient().start(instant);
            }

            LOG.info(
                    "Committing Hudi instant {} with {} write status entries and metadata {}.",
                    instant,
                    statuses.size(),
                    commitMetadata);
            boolean committed = writeClient.commit(instant, statuses, Option.of(commitMetadata));
            if (!committed) {
                ckpMetadata.abortInstant(instant);
                throw new IOException("Failed to commit Hudi instant " + instant + ".");
            }

            ckpMetadata.commitInstant(instant);
            LOG.info("Committed Hudi instant {} successfully.", instant);
            return LakeCommitResult.committedIsReadable(Long.parseLong(instant));
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("Failed to commit Hudi instant " + instant + ".", e);
        }
    }

    @Override
    public void abort(HudiCommittable committable) throws IOException {
        Set<String> instants = new LinkedHashSet<>(committable.getWriteStatuses().keySet());
        instants.addAll(committable.getCompactionWriteStatuses().keySet());
        for (String instant : instants) {
            try {
                writeClient.rollback(instant);
                ckpMetadata.abortInstant(instant);
                LOG.info("Aborted Hudi instant {}.", instant);
            } catch (Exception e) {
                throw new IOException("Failed to abort Hudi instant " + instant + ".", e);
            }
        }
    }

    private static void ensureNoCompactionWriteStatuses(HudiCommittable committable)
            throws IOException {
        Map<String, List<WriteStatus>> compactionWriteStatuses =
                committable.getCompactionWriteStatuses();
        if (!compactionWriteStatuses.isEmpty()) {
            throw new IOException(
                    "Hudi compaction write statuses are not supported yet, but got instants "
                            + compactionWriteStatuses.keySet()
                            + ".");
        }
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        HoodieTimeline latestLakeTimeline =
                getCompletedTimelineCommittedBy(FLUSS_LAKE_TIERING_COMMIT_USER);
        Optional<HoodieInstant> latestLakeInstant =
                latestLakeTimeline.getReverseOrderedInstantsByCompletionTime().findFirst();
        if (!latestLakeInstant.isPresent()) {
            return null;
        }

        long latestLakeSnapshotId = Long.parseLong(latestLakeInstant.get().requestedTime());
        if (latestLakeSnapshotIdOfFluss != null
                && latestLakeSnapshotId <= latestLakeSnapshotIdOfFluss) {
            return null;
        }

        HoodieCommitMetadata metadata =
                latestLakeTimeline.readCommitMetadata(latestLakeInstant.get());
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

    private void validateWriteStatuses(String instant, List<WriteStatus> writeStatuses) {
        long totalErrorRecords = 0L;
        for (WriteStatus writeStatus : writeStatuses) {
            totalErrorRecords += writeStatus.getTotalErrorRecords();
        }
        if (totalErrorRecords > 0
                && !hudiTableInfo.getFlinkConfig().get(FlinkOptions.IGNORE_FAILED)) {
            throw new HoodieException(
                    String.format(
                            "Commit Hudi instant %s failed with %s error records.",
                            instant, totalErrorRecords));
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
            Map<String, String> extraMetadata = metadata.getExtraMetadata();
            return extraMetadata != null && commitUser.equals(extraMetadata.get(COMMITTER_USER));
        } catch (IOException e) {
            // a read failure must not be silently treated as "not committed by Fluss",
            // otherwise we may miss an already-tiered snapshot and re-commit duplicated data
            throw new UncheckedIOException(
                    "Failed to read Hudi commit metadata for instant " + instant + ".", e);
        }
    }
}
