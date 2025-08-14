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

package com.alibaba.fluss.lake.iceberg.tiering;

import com.alibaba.fluss.lake.committer.BucketOffset;
import com.alibaba.fluss.lake.committer.CommittedLakeSnapshot;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import com.alibaba.fluss.utils.json.BucketOffsetJsonSerde;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.io.WriteResult;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.lake.committer.BucketOffset.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static com.alibaba.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static com.alibaba.fluss.lake.writer.LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Implementation of {@link LakeCommitter} for Iceberg. */
public class IcebergLakeCommitter implements LakeCommitter<IcebergWriteResult, IcebergCommittable> {

    private final Catalog icebergCatalog;
    private final Table icebergTable;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ThreadLocal<Long> currentCommitSnapshotId = new ThreadLocal<>();

    public IcebergLakeCommitter(IcebergCatalogProvider icebergCatalogProvider, TablePath tablePath)
            throws IOException {
        this.icebergCatalog = icebergCatalogProvider.get();
        this.icebergTable = getTable(tablePath);
        // register iceberg listener
        Listeners.register(new IcebergSnapshotCreateListener(), CreateSnapshotEvent.class);
    }

    @Override
    public IcebergCommittable toCommittable(List<IcebergWriteResult> icebergWriteResults) {
        // Aggregate all write results into a single committable
        IcebergCommittable.Builder builder = IcebergCommittable.builder();

        for (IcebergWriteResult result : icebergWriteResults) {
            WriteResult writeResult = result.getWriteResult();

            // Add data files
            for (DataFile dataFile : writeResult.dataFiles()) {
                builder.addDataFile(dataFile);
            }
        }

        return builder.build();
    }

    @Override
    public long commit(IcebergCommittable committable, Map<String, String> snapshotProperties)
            throws IOException {
        try {
            // Refresh table to get latest metadata
            icebergTable.refresh();
            // Simple append-only case: only data files, no delete files or compaction
            AppendFiles appendFiles = icebergTable.newAppend();
            for (DataFile dataFile : committable.getDataFiles()) {
                appendFiles.appendFile(dataFile);
            }
            if (!committable.getDeleteFiles().isEmpty()) {
                throw new IllegalStateException(
                        "Delete files are not supported in append-only mode. "
                                + "Found "
                                + committable.getDeleteFiles().size()
                                + " delete files.");
            }

            addFlussProperties(appendFiles, snapshotProperties);

            appendFiles.commit();

            Long commitSnapshotId = currentCommitSnapshotId.get();
            currentCommitSnapshotId.remove();

            return checkNotNull(
                    commitSnapshotId, "Iceberg committed snapshot id must be non-null.");
        } catch (Exception e) {
            throw new IOException("Failed to commit to Iceberg table.", e);
        }
    }

    private void addFlussProperties(
            AppendFiles appendFiles, Map<String, String> snapshotProperties) {
        appendFiles.set("commit-user", FLUSS_LAKE_TIERING_COMMIT_USER);
        for (Map.Entry<String, String> entry : snapshotProperties.entrySet()) {
            appendFiles.set(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void abort(IcebergCommittable committable) {
        List<String> filesToDelete =
                committable.getDataFiles().stream()
                        .map(dataFile -> dataFile.path().toString())
                        .collect(Collectors.toList());
        CatalogUtil.deleteFiles(icebergTable.io(), filesToDelete, "data file", true);
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        // todo: may refactor to common methods?
        Snapshot latestLakeSnapshot =
                getCommittedLatestSnapshotOfLake(FLUSS_LAKE_TIERING_COMMIT_USER);

        if (latestLakeSnapshot == null) {
            return null;
        }

        // Check if there's a gap between Fluss and Iceberg snapshots
        if (latestLakeSnapshotIdOfFluss != null
                && latestLakeSnapshot.snapshotId() <= latestLakeSnapshotIdOfFluss) {
            return null;
        }

        CommittedLakeSnapshot committedLakeSnapshot =
                new CommittedLakeSnapshot(latestLakeSnapshot.snapshotId());

        // Reconstruct bucket offsets from snapshot properties
        Map<String, String> properties = latestLakeSnapshot.summary();
        if (properties == null) {
            throw new IOException(
                    "Failed to load committed lake snapshot properties from Iceberg.");
        }

        String flussOffsetProperties = properties.get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
        if (flussOffsetProperties == null) {
            throw new IllegalArgumentException(
                    "Cannot resume tiering from snapshot without bucket offset properties. "
                            + "The snapshot was committed to Iceberg but missing Fluss metadata.");
        }

        for (JsonNode node : OBJECT_MAPPER.readTree(flussOffsetProperties)) {
            BucketOffset bucketOffset = BucketOffsetJsonSerde.INSTANCE.deserialize(node);
            if (bucketOffset.getPartitionId() != null) {
                committedLakeSnapshot.addPartitionBucket(
                        bucketOffset.getPartitionId(),
                        bucketOffset.getPartitionQualifiedName(),
                        bucketOffset.getBucket(),
                        bucketOffset.getLogOffset());
            } else {
                committedLakeSnapshot.addBucket(
                        bucketOffset.getBucket(), bucketOffset.getLogOffset());
            }
        }

        return committedLakeSnapshot;
    }

    @Override
    public void close() throws Exception {
        try {
            if (icebergCatalog != null && icebergCatalog instanceof AutoCloseable) {
                ((AutoCloseable) icebergCatalog).close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close IcebergLakeCommitter.", e);
        }
    }

    private Table getTable(TablePath tablePath) throws IOException {
        try {
            TableIdentifier tableId = toIceberg(tablePath);
            return icebergCatalog.loadTable(tableId);
        } catch (Exception e) {
            throw new IOException("Failed to get table " + tablePath + " in Iceberg.", e);
        }
    }

    @Nullable
    private Snapshot getCommittedLatestSnapshotOfLake(String commitUser) {
        icebergTable.refresh();

        // Find the latest snapshot committed by Fluss
        Iterable<Snapshot> snapshots = icebergTable.snapshots();
        Snapshot latestFlussSnapshot = null;

        for (Snapshot snapshot : snapshots) {
            Map<String, String> summary = snapshot.summary();
            if (summary != null && commitUser.equals(summary.get("commit-user"))) {
                if (latestFlussSnapshot == null
                        || snapshot.snapshotId() > latestFlussSnapshot.snapshotId()) {
                    latestFlussSnapshot = snapshot;
                }
            }
        }

        return latestFlussSnapshot;
    }

    /** A {@link Listener} to listen the iceberg create snapshot event. */
    public static class IcebergSnapshotCreateListener implements Listener<CreateSnapshotEvent> {
        @Override
        public void notify(CreateSnapshotEvent createSnapshotEvent) {
            currentCommitSnapshotId.set(createSnapshotEvent.snapshotId());
        }
    }
}
