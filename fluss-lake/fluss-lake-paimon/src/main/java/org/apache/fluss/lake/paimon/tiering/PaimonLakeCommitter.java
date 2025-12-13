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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.lake.committer.BucketOffset;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.utils.json.BucketOffsetJsonSerde;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.committer.BucketOffset.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;

/** Implementation of {@link LakeCommitter} for Paimon. */
public class PaimonLakeCommitter implements LakeCommitter<PaimonWriteResult, PaimonCommittable> {

    private final Catalog paimonCatalog;
    private final FileStoreTable fileStoreTable;
    private FileStoreCommit fileStoreCommit;
    private static final ThreadLocal<Long> currentCommitSnapshotId = new ThreadLocal<>();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public PaimonLakeCommitter(PaimonCatalogProvider paimonCatalogProvider, TablePath tablePath)
            throws IOException {
        this.paimonCatalog = paimonCatalogProvider.get();
        this.fileStoreTable = getTable(tablePath);
    }

    @Override
    public PaimonCommittable toCommittable(List<PaimonWriteResult> paimonWriteResults)
            throws IOException {
        ManifestCommittable committable = new ManifestCommittable(COMMIT_IDENTIFIER);
        for (PaimonWriteResult paimonWriteResult : paimonWriteResults) {
            committable.addFileCommittable(paimonWriteResult.commitMessage());
        }
        return new PaimonCommittable(committable);
    }

    @Override
    public long commit(PaimonCommittable committable, Map<String, String> snapshotProperties)
            throws IOException {
        ManifestCommittable manifestCommittable = committable.manifestCommittable();
        snapshotProperties.forEach(manifestCommittable::addProperty);

        try {
            fileStoreCommit =
                    fileStoreTable
                            .store()
                            .newCommit(FLUSS_LAKE_TIERING_COMMIT_USER, fileStoreTable);
            fileStoreCommit.commit(manifestCommittable, false);
            Long commitSnapshotId = currentCommitSnapshotId.get();
            currentCommitSnapshotId.remove();

            return checkNotNull(commitSnapshotId, "Paimon committed snapshot id must be non-null.");
        } catch (Throwable t) {
            if (fileStoreCommit != null) {
                // if any error happen while commit, abort the commit to clean committable
                fileStoreCommit.abort(manifestCommittable.fileCommittables());
            }
            throw new IOException(t);
        }
    }

    @Override
    public void abort(PaimonCommittable committable) throws IOException {
        fileStoreCommit =
                fileStoreTable.store().newCommit(FLUSS_LAKE_TIERING_COMMIT_USER, fileStoreTable);
        fileStoreCommit.abort(committable.manifestCommittable().fileCommittables());
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        Snapshot latestLakeSnapshotOfLake =
                getCommittedLatestSnapshotOfLake(FLUSS_LAKE_TIERING_COMMIT_USER);
        if (latestLakeSnapshotOfLake == null) {
            return null;
        }

        // we get the latest snapshot committed by fluss,
        // but the latest snapshot is not greater than latestLakeSnapshotIdOfFluss, no any missing
        // snapshot, return directly
        if (latestLakeSnapshotIdOfFluss != null
                && latestLakeSnapshotOfLake.id() <= latestLakeSnapshotIdOfFluss) {
            return null;
        }

        CommittedLakeSnapshot committedLakeSnapshot =
                new CommittedLakeSnapshot(latestLakeSnapshotOfLake.id());

        if (latestLakeSnapshotOfLake.properties() == null) {
            throw new IOException("Failed to load committed lake snapshot properties from Paimon.");
        }

        // if resume from an old tiering service v0.7 without paimon supporting snapshot properties,
        // we can't get the properties. But once come into here, it must be that
        // tiering service commit snapshot to lake, but fail to commit to fluss, we have to notify
        // users to run old tiering service again to commit the snapshot to fluss again, and then
        // it can resume tiering with new tiering service
        Map<String, String> lakeSnapshotProperties = latestLakeSnapshotOfLake.properties();
        if (lakeSnapshotProperties == null) {
            throw new IllegalArgumentException(
                    "Cannot resume tiering from an old version(v0.7) of tiering service. "
                            + "The snapshot was committed to the lake storage but failed to commit to Fluss. "
                            + "To resolve this:\n"
                            + "1. Run the old tiering service(v0.7) again to complete the Fluss commit\n"
                            + "2. Then you can resume tiering with the newer version of tiering service");
        } else {
            String flussOffsetProperties =
                    lakeSnapshotProperties.get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
            for (JsonNode node : OBJECT_MAPPER.readTree(flussOffsetProperties)) {
                BucketOffset bucketOffset = BucketOffsetJsonSerde.INSTANCE.deserialize(node);
                if (bucketOffset.getPartitionId() != null) {
                    committedLakeSnapshot.addPartitionBucket(
                            bucketOffset.getPartitionId(),
                            bucketOffset.getBucket(),
                            bucketOffset.getLogOffset());
                } else {
                    committedLakeSnapshot.addBucket(
                            bucketOffset.getBucket(), bucketOffset.getLogOffset());
                }
            }
        }
        return committedLakeSnapshot;
    }

    @Nullable
    private Snapshot getCommittedLatestSnapshotOfLake(String commitUser) throws IOException {
        // get the latest snapshot committed by fluss or latest committed id
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        Long userCommittedSnapshotIdOrLatestCommitId =
                fileStoreTable
                        .snapshotManager()
                        .pickOrLatest((snapshot -> snapshot.commitUser().equals(commitUser)));
        // no any snapshot, return null directly
        if (userCommittedSnapshotIdOrLatestCommitId == null) {
            return null;
        }

        // pick the snapshot
        Snapshot snapshot = snapshotManager.tryGetSnapshot(userCommittedSnapshotIdOrLatestCommitId);

        if (!snapshot.commitUser().equals(commitUser)) {
            // the snapshot is still not committed by Fluss, return directly
            return null;
        }
        return snapshot;
    }

    @Override
    public void close() throws Exception {
        try {
            if (fileStoreCommit != null) {
                fileStoreCommit.close();
            }
            if (paimonCatalog != null) {
                paimonCatalog.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close PaimonLakeCommitter.", e);
        }
    }

    private FileStoreTable getTable(TablePath tablePath) throws IOException {
        try {
            FileStoreTable table =
                    (FileStoreTable)
                            paimonCatalog
                                    .getTable(toPaimon(tablePath))
                                    .copy(
                                            Collections.singletonMap(
                                                    CoreOptions.COMMIT_CALLBACKS.key(),
                                                    PaimonLakeCommitter.PaimonCommitCallback.class
                                                            .getName()));

            return table;
        } catch (Exception e) {
            throw new IOException("Failed to get table " + tablePath + " in Paimon.", e);
        }
    }

    /** A {@link CommitCallback} to save paimon commit snapshot info. */
    public static class PaimonCommitCallback implements CommitCallback {

        @Override
        public void call(
                List<SimpleFileEntry> baseFiles,
                List<ManifestEntry> deltaFiles,
                List<IndexManifestEntry> indexFiles,
                Snapshot snapshot) {
            currentCommitSnapshotId.set(snapshot.id());
        }

        @Override
        public void retry(ManifestCommittable manifestCommittable) {
            // do-nothing
        }

        @Override
        public void close() throws Exception {
            // do-nothing
        }
    }
}
