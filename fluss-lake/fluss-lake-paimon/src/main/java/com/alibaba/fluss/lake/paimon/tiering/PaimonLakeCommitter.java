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

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.lake.committer.CommittedLakeSnapshot;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.alibaba.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static com.alibaba.fluss.metadata.ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR;
import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;

/** Implementation of {@link LakeCommitter} for Paimon. */
public class PaimonLakeCommitter implements LakeCommitter<PaimonWriteResult, PaimonCommittable> {

    private final Catalog paimonCatalog;
    private final FileStoreTable fileStoreTable;
    private FileStoreCommit fileStoreCommit;
    private final TablePath tablePath;

    public PaimonLakeCommitter(PaimonCatalogProvider paimonCatalogProvider, TablePath tablePath)
            throws IOException {
        this.paimonCatalog = paimonCatalogProvider.get();
        this.fileStoreTable = getTable(tablePath);
        this.tablePath = tablePath;
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
    public long commit(PaimonCommittable committable) throws IOException {
        ManifestCommittable manifestCommittable = committable.manifestCommittable();
        PaimonCommitCallback paimonCommitCallback = new PaimonCommitCallback();
        try {
            fileStoreCommit =
                    fileStoreTable
                            .store()
                            .newCommit(
                                    FLUSS_LAKE_TIERING_COMMIT_USER,
                                    Collections.singletonList(paimonCommitCallback));
            fileStoreCommit.commit(manifestCommittable, Collections.emptyMap());
            return checkNotNull(
                    paimonCommitCallback.commitSnapshotId,
                    "Paimon committed snapshot id must be non-null.");
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
        fileStoreCommit = fileStoreTable.store().newCommit(FLUSS_LAKE_TIERING_COMMIT_USER);
        fileStoreCommit.abort(committable.manifestCommittable().fileCommittables());
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        Long latestLakeSnapshotIdOfLake =
                getCommittedLatestSnapshotIdOfLake(FLUSS_LAKE_TIERING_COMMIT_USER);
        if (latestLakeSnapshotIdOfLake == null) {
            return null;
        }

        // we get the latest snapshot committed by fluss,
        // but the latest snapshot is not greater than latestLakeSnapshotIdOfFluss, no any missing
        // snapshot, return directly
        if (latestLakeSnapshotIdOfFluss != null
                && latestLakeSnapshotIdOfLake <= latestLakeSnapshotIdOfFluss) {
            return null;
        }

        // todo: the temporary way to scan the delta to get the log end offset,
        // we should read from snapshot's properties in Paimon 1.2
        CommittedLakeSnapshot committedLakeSnapshot =
                new CommittedLakeSnapshot(latestLakeSnapshotIdOfLake);
        ScanMode scanMode =
                fileStoreTable.primaryKeys().isEmpty() ? ScanMode.DELTA : ScanMode.CHANGELOG;

        Iterator<ManifestEntry> manifestEntryIterator =
                fileStoreTable
                        .store()
                        .newScan()
                        .withSnapshot(latestLakeSnapshotIdOfLake)
                        .withKind(scanMode)
                        .readFileIterator();

        int bucketIdColumnIndex = getColumnIndex(BUCKET_COLUMN_NAME);
        int logOffsetColumnIndex = getColumnIndex(OFFSET_COLUMN_NAME);
        while (manifestEntryIterator.hasNext()) {
            updateCommittedLakeSnapshot(
                    committedLakeSnapshot,
                    manifestEntryIterator.next(),
                    bucketIdColumnIndex,
                    logOffsetColumnIndex);
        }

        return committedLakeSnapshot;
    }

    @Nullable
    private Long getCommittedLatestSnapshotIdOfLake(String commitUser) throws IOException {
        // get the latest snapshot commited by fluss or latest commited id
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
            // the snapshot is still not commited by Fluss, return directly
            return null;
        }
        return snapshot.id();
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
            return (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        } catch (Exception e) {
            throw new IOException("Failed to get table " + tablePath + " in Paimon.", e);
        }
    }

    private static class PaimonCommitCallback implements CommitCallback {

        private Long commitSnapshotId = null;

        @Override
        public void call(List<ManifestEntry> list, Snapshot snapshot) {
            this.commitSnapshotId = snapshot.id();
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

    private void updateCommittedLakeSnapshot(
            CommittedLakeSnapshot committedLakeSnapshot,
            ManifestEntry manifestEntry,
            int bucketIdColumnIndex,
            int logOffsetColumnIndex) {
        // always get bucket, log_offset from statistic
        DataFileMeta dataFileMeta = manifestEntry.file();
        BinaryRow maxStatisticRow = dataFileMeta.valueStats().maxValues();

        int bucketId = maxStatisticRow.getInt(bucketIdColumnIndex);
        long offset = maxStatisticRow.getLong(logOffsetColumnIndex);

        String partition = null;
        BinaryRow partitionRow = manifestEntry.partition();
        if (partitionRow.getFieldCount() > 0) {
            List<String> partitionFields = new ArrayList<>(partitionRow.getFieldCount());
            for (int i = 0; i < partitionRow.getFieldCount(); i++) {
                partitionFields.add(partitionRow.getString(i).toString());
            }
            partition = String.join(PARTITION_SPEC_SEPARATOR, partitionFields);
        }

        if (partition == null) {
            committedLakeSnapshot.addBucket(bucketId, offset);
        } else {
            committedLakeSnapshot.addPartitionBucket(partition, bucketId, offset);
        }
    }

    private int getColumnIndex(String columnName) {
        int columnIndex = fileStoreTable.schema().fieldNames().indexOf(columnName);
        if (columnIndex < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Column '%s' is not found in paimon table %s, the columns of the table are %s",
                            columnIndex, tablePath, fileStoreTable.schema().fieldNames()));
        }
        return columnIndex;
    }
}
