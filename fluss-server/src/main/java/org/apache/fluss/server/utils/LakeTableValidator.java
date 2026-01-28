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

package org.apache.fluss.server.utils;

import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeSnapshotInfo;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.json.TableBucketOffsets;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/** Helper for validating data lake tables. */
public final class LakeTableValidator {

    /**
     * Perform validation before enabling datalake for an existing table.
     *
     * <p>The validation focuses on two aspects:
     *
     * <ul>
     *   <li>case1: snapshot consistency between Fluss and the external lake table
     *   <li>case2: TTL risk when the latest lake snapshot is much older than {@code table.log.ttl}
     * </ul>
     */
    public static void validateBeforeEnableDataLake(
            TableInfo tableInfo,
            LakeCatalog lakeCatalog,
            LakeCatalog.Context lakeContext,
            ZooKeeperClient zkClient,
            Clock clock)
            throws Exception {
        long tableId = tableInfo.getTableId();
        TablePath tablePath = tableInfo.getTablePath();

        Optional<LakeTable> lakeTableOptional = zkClient.getLakeTable(tableId);
        Optional<LakeSnapshotInfo> latestSnapshotInfoOptional =
                lakeCatalog.getLatestSnapshotInfo(tablePath, lakeContext);

        // If the latest snapshot info is LakeSnapshotInfo.EMPTY,
        // it means the lake catalog does not support get latest snapshot info
        if (latestSnapshotInfoOptional.equals(Optional.of(LakeSnapshotInfo.EMPTY))) {
            return;
        }

        checkSnapshotConsistency(tablePath, tableId, lakeTableOptional, latestSnapshotInfoOptional);
        checkTtlRisk(tableInfo, latestSnapshotInfoOptional, clock);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static void checkSnapshotConsistency(
            TablePath tablePath,
            long tableId,
            Optional<LakeTable> lakeTableOptional,
            Optional<LakeSnapshotInfo> latestSnapshotInfoOptional)
            throws IOException {
        if (!latestSnapshotInfoOptional.isPresent()) {
            // No snapshot information from lake side.
            // - If Fluss side also has no LakeTable, this is treated as first-time enable.
            // - If Fluss side has LakeTable metadata but lake side has no snapshot info,
            //   consider it as an inconsistent state (for example, lake table or snapshots
            //   were removed manually).
            if (!lakeTableOptional.isPresent()) {
                return;
            }

            throw new InvalidAlterTableException(
                    String.format(
                            "Cannot enable datalake for table %s because Fluss has lake "
                                    + "snapshot metadata but the lake table has no snapshot information. "
                                    + "This usually means the lake table or its snapshots were manually "
                                    + "removed. Please recreate or clean the lake table and try again.",
                            tablePath));
        }

        LakeSnapshotInfo lakeSnapshotInfo = latestSnapshotInfoOptional.get();
        String flussOffsets = lakeSnapshotInfo.getFlussOffsetsProperty();

        if (flussOffsets == null) {
            // There is a snapshot but no fluss-offsets property; most likely the snapshot was not
            // produced by Fluss tiering.
            throw new InvalidAlterTableException(
                    String.format(
                            "Cannot enable datalake for table %s because existing lake table "
                                    + "has snapshots without '%s' property. This usually means "
                                    + "data was written directly into the lake table. Please recreate "
                                    + "or clean the lake table and try again.",
                            tablePath, LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY));
        }

        long tableIdFromOffsets = extractTableIdFromOffsetsProperty(flussOffsets);
        if (tableIdFromOffsets != tableId) {
            throw new InvalidAlterTableException(
                    String.format(
                            "Cannot enable datalake for table %s because the latest lake snapshot "
                                    + "belongs to a different Fluss table (tableId in offsets path: %d, "
                                    + "current tableId: %d). Please ensure the lake table is created "
                                    + "and written by the same Fluss table.",
                            tablePath, tableIdFromOffsets, tableId));
        }

        // If Fluss side also has LakeTable, we can further verify snapshot id alignment.
        if (lakeTableOptional.isPresent()) {
            LakeTable lakeTable = lakeTableOptional.get();
            LakeTableSnapshot snapshot = lakeTable.getOrReadLatestTableSnapshot();
            if (snapshot.getSnapshotId() != lakeSnapshotInfo.getSnapshotId()) {
                throw new InvalidAlterTableException(
                        String.format(
                                "Cannot enable datalake for table %s because Fluss and lake "
                                        + "have different latest snapshot ids (fluss: %d, lake: %d).",
                                tablePath,
                                snapshot.getSnapshotId(),
                                lakeSnapshotInfo.getSnapshotId()));
            }
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static void checkTtlRisk(
            TableInfo tableInfo,
            Optional<LakeSnapshotInfo> latestSnapshotInfoOptional,
            Clock clock) {
        TableConfig tableConfig = tableInfo.getTableConfig();
        long ttlMs = tableConfig.getLogTTLMs();
        if (ttlMs <= 0) {
            // TTL disabled (e.g. -1), skip TTL checks.
            return;
        }

        if (!latestSnapshotInfoOptional.isPresent()) {
            // No snapshot info from lake side, cannot judge TTL risk, skip.
            return;
        }

        LakeSnapshotInfo snapshotInfo = latestSnapshotInfoOptional.get();
        long snapshotTimeMillis = snapshotInfo.getCommitTimestampMillis();
        if (snapshotTimeMillis <= 0) {
            // Unknown commit timestamp, skip TTL checks.
            return;
        }

        long now = clock.milliseconds();
        long diff = now - snapshotTimeMillis;

        // Note: This is a lossy check, but most high-risk scenarios have been filtered out
        if (diff > ttlMs) {
            throw new InvalidAlterTableException(
                    String.format(
                            "Cannot enable datalake for table %s because the latest lake snapshot "
                                    + "is older than table.log.ttl (snapshot age: %d ms, ttl: %d ms). "
                                    + "Enabling datalake may cause tiering to fail when restoring from "
                                    + "expired offsets.",
                            tableInfo.getTablePath(), diff, ttlMs));
        }
    }

    public static long extractTableIdFromOffsetsProperty(String offsets) {
        String trimmed = offsets.trim();
        if (trimmed.isEmpty()) {
            throw new InvalidAlterTableException(
                    String.format(
                            "Invalid '%s' property: empty value.",
                            LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY));
        }

        // v1: JSON format, deserialize TableBucketOffsets and read tableId.
        if (trimmed.startsWith("{")) {
            TableBucketOffsets tableBucketOffsets =
                    TableBucketOffsets.fromJsonBytes(trimmed.getBytes(StandardCharsets.UTF_8));
            return tableBucketOffsets.getTableId();
        }

        // v2: path format, e.g.
        // {$remote.data.dir}/lake/{databaseName}/{tableName}-{tableId}/metadata/{UUID}.offsets
        int metadataIdx = trimmed.indexOf("/metadata/");
        if (metadataIdx <= 0) {
            throw new InvalidAlterTableException(
                    String.format(
                            "Invalid '%s' path: %s. Expected to contain '/metadata/'.",
                            LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, offsets));
        }

        String tableDir = trimmed.substring(0, metadataIdx);
        int lastSlashIdx = tableDir.lastIndexOf('/');
        if (lastSlashIdx < 0 || lastSlashIdx == tableDir.length() - 1) {
            throw new InvalidAlterTableException(
                    String.format(
                            "Invalid '%s' path: %s. Cannot extract table name part.",
                            LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, offsets));
        }

        String tableSegment = tableDir.substring(lastSlashIdx + 1);
        int dashIdx = tableSegment.lastIndexOf('-');
        if (dashIdx < 0 || dashIdx == tableSegment.length() - 1) {
            throw new InvalidAlterTableException(
                    String.format(
                            "Invalid '%s' path: %s. Cannot extract tableId part.",
                            LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, offsets));
        }

        try {
            return Long.parseLong(tableSegment.substring(dashIdx + 1));
        } catch (NumberFormatException e) {
            throw new InvalidAlterTableException(
                    String.format(
                            "Invalid '%s' path: %s. tableId is not a number.",
                            LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, offsets),
                    e);
        }
    }
}
