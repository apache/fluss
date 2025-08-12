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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.PropertyUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.lake.iceberg.tiering.IcebergLakeTieringFactory.FLUSS_BUCKET_OFFSET_PROPERTY;
import static com.alibaba.fluss.lake.iceberg.tiering.IcebergLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static com.alibaba.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;

/** Implementation of {@link LakeCommitter} for Iceberg. */
public class IcebergLakeCommitter implements LakeCommitter<IcebergWriteResult, IcebergCommittable> {

    private static final String AUTO_MAINTENANCE_PROPERTY = "table.datalake.auto-maintenance";
    private static final String COMMIT_RETRY_PROPERTY = "commit.retry.num-retries";
    private static final int DEFAULT_COMMIT_RETRIES = 4;

    private final Catalog icebergCatalog;
    private final Table icebergTable;
    private final TablePath tablePath;
    private final boolean autoMaintenance;
    private final int commitRetries;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public IcebergLakeCommitter(IcebergCatalogProvider icebergCatalogProvider, TablePath tablePath)
            throws IOException {
        this.icebergCatalog = icebergCatalogProvider.get();
        this.icebergTable = getTable(tablePath);
        this.tablePath = tablePath;
        this.autoMaintenance =
                PropertyUtil.propertyAsBoolean(
                        icebergTable.properties(), AUTO_MAINTENANCE_PROPERTY, false);
        this.commitRetries =
                PropertyUtil.propertyAsInt(
                        icebergTable.properties(), COMMIT_RETRY_PROPERTY, DEFAULT_COMMIT_RETRIES);
    }

    @Override
    public IcebergCommittable toCommittable(List<IcebergWriteResult> icebergWriteResults)
            throws IOException {
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

            long snapshotId = commitWithRetry(appendFiles);

            return snapshotId;

        } catch (Exception e) {
            throw new IOException("Failed to commit to Iceberg table.", e);
        }
    }

    private void addFlussProperties(
            AppendFiles appendFiles, Map<String, String> snapshotProperties) {
        appendFiles.set("commit-user", FLUSS_LAKE_TIERING_COMMIT_USER);

        if (snapshotProperties.containsKey(FLUSS_BUCKET_OFFSET_PROPERTY)) {
            appendFiles.set(
                    FLUSS_BUCKET_OFFSET_PROPERTY,
                    snapshotProperties.get(FLUSS_BUCKET_OFFSET_PROPERTY));
        }

        for (Map.Entry<String, String> entry : snapshotProperties.entrySet()) {
            if (!FLUSS_BUCKET_OFFSET_PROPERTY.equals(entry.getKey())) {
                appendFiles.set(entry.getKey(), entry.getValue());
            }
        }
    }

    private long commitWithRetry(AppendFiles appendFiles) throws IOException {
        Exception lastException = null;

        for (int attempt = 0; attempt <= commitRetries; attempt++) {
            try {
                appendFiles.commit();
                return icebergTable.currentSnapshot().snapshotId();
            } catch (Exception e) {
                lastException = e;
                if (attempt < commitRetries) {
                    icebergTable.refresh();
                    try {
                        Thread.sleep(100 * (attempt + 1)); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Commit interrupted", ie);
                    }
                }
            }
        }

        throw new IOException(
                "Failed to commit AppendFiles after " + (commitRetries + 1) + " attempts",
                lastException);
    }

    @Override
    public void abort(IcebergCommittable committable) throws IOException {
        // For Iceberg, we don't need to explicitly abort since files are not
        // visible until committed. The files will be cleaned up by table maintenance.
        // TODO: Discuss with Yuxia, Consider deleting uncommitted files explicitly for immediate
        // cleanup
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
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

        String flussOffsetProperties = properties.get(FLUSS_BUCKET_OFFSET_PROPERTY);
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

    private void performSnapshotExpiration() {
        try {
            ExpireSnapshots expireAction = icebergTable.expireSnapshots();
            // TODO: Configure expiration policy based on table properties discuss with Yuxia
            // expireAction.expireOlderThan(System.currentTimeMillis() - retentionPeriod);
            expireAction.commit();
        } catch (Exception e) {
            System.err.println("Snapshot expiration failed: " + e.getMessage());
        }
    }
}
