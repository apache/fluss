/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.source.SupportsHistoricalSnapshot.HistoricalSnapshot;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.json.TableBucketOffsets;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toFlussRowType;

/** Selects an offset-compatible complete historical base without scanning table data. */
final class PaimonHistoricalSnapshotSelector {
    private static final Logger LOG =
            LoggerFactory.getLogger(PaimonHistoricalSnapshotSelector.class);

    private final FileStoreTable table;

    PaimonHistoricalSnapshotSelector(FileStoreTable table) {
        this.table = table;
    }

    Optional<HistoricalSnapshot> select(
            TableInfo tableInfo, Map<TableBucket, Long> earliest, Map<TableBucket, Long> target)
            throws IOException {
        if (!toFlussRowType(table.rowType()).equals(tableInfo.getRowType())
                || !table.primaryKeys().equals(tableInfo.getPrimaryKeys())
                || !table.partitionKeys().isEmpty()) {
            throw new IOException(
                    "Historical snapshot schema or keys differ from Fluss table "
                            + tableInfo.getTableId());
        }
        if (!table.primaryKeys().isEmpty()
                && (!"deduplicate"
                                .equals(table.options().getOrDefault("merge-engine", "deduplicate"))
                        || Integer.parseInt(table.options().getOrDefault("bucket", "-1"))
                                != tableInfo.getNumBuckets())) {
            throw new IOException(
                    "Historical reads require deduplicate merge engine and stable buckets");
        }
        SnapshotManager manager = table.snapshotManager();
        Long latest = manager.latestSnapshotId();
        Long first = manager.earliestSnapshotId();
        if (latest == null || first == null) {
            return Optional.empty();
        }
        List<Snapshot> snapshots = new ArrayList<>();
        // Validate the whole retained lineage before accepting a candidate. A Fluss commit can
        // otherwise inherit data from a preceding foreign writer or an overwrite.
        for (long id = latest; id >= first; id--) {
            Snapshot snapshot;
            try {
                snapshot = manager.tryGetSnapshot(id);
            } catch (FileNotFoundException e) {
                continue;
            }
            if (snapshot == null) {
                continue;
            }
            if (snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE
                    || (snapshot.commitKind() == Snapshot.CommitKind.APPEND
                            && !snapshot.commitUser().startsWith(FLUSS_LAKE_TIERING_COMMIT_USER))) {
                throw new IOException(
                        "Unsupported historical lineage: snapshot "
                                + id
                                + " was written by "
                                + snapshot.commitUser()
                                + " with "
                                + snapshot.commitKind());
            }
            snapshots.add(snapshot);
        }
        Map<String, TableBucketOffsets> cachedOffsets = new HashMap<>();
        for (Snapshot snapshot : snapshots) {
            if (snapshot.schemaId() != table.schema().id()) {
                continue;
            }
            String offsetsPath =
                    snapshot.properties() == null
                            ? null
                            : snapshot.properties().get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
            if (offsetsPath == null) {
                continue;
            }
            TableBucketOffsets offsets = cachedOffsets.get(offsetsPath);
            if (offsets == null) {
                try {
                    offsets = readOffsets(offsetsPath);
                } catch (FileNotFoundException e) {
                    throw new IOException(
                            "Missing fluss-offsets file: table="
                                    + tableInfo.getTablePath()
                                    + ", tableId="
                                    + tableInfo.getTableId()
                                    + ", snapshotId="
                                    + snapshot.id()
                                    + ", path="
                                    + offsetsPath,
                            e);
                }
                cachedOffsets.put(offsetsPath, offsets);
            }
            if (offsets.getTableId() != tableInfo.getTableId()) {
                throw new IOException(
                        "Historical offsets table identity mismatch at snapshot " + snapshot.id());
            }
            if (!compatible(offsets.getOffsets(), earliest, target)) {
                continue;
            }
            if (Boolean.parseBoolean(
                            table.options().getOrDefault("deletion-vectors.enabled", "false"))
                    && table
                            .store()
                            .newScan()
                            .withSnapshot(snapshot.id())
                            .plan()
                            .files(FileKind.ADD)
                            .stream()
                            .anyMatch(entry -> entry.level() == 0)) {
                // The normal DV reader skips L0. Tiered offsets are readable only if no L0 exists.
                continue;
            }
            LOG.info(
                    "Selected historical snapshot {} for table {} after checking {} retained versions",
                    snapshot.id(),
                    tableInfo.getTableId(),
                    snapshots.size());
            return Optional.of(new HistoricalSnapshot(snapshot.id(), offsets.getOffsets()));
        }
        return Optional.empty();
    }

    private TableBucketOffsets readOffsets(String value) throws IOException {
        if (value.trim().startsWith("{")) {
            throw new IOException("Unsupported legacy embedded historical offsets: " + value);
        }
        FsPath path = new FsPath(value);
        try (FSDataInputStream in = path.getFileSystem().open(path);
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(in, out, false);
            try {
                return TableBucketOffsets.fromJsonBytes(out.toByteArray());
            } catch (RuntimeException e) {
                throw new IOException("Corrupt historical offsets file " + path, e);
            }
        }
    }

    static boolean compatible(
            Map<TableBucket, Long> offsets,
            Map<TableBucket, Long> earliest,
            Map<TableBucket, Long> target) {
        if (!offsets.keySet().equals(target.keySet())
                || !earliest.keySet().equals(target.keySet())) {
            return false;
        }
        for (Map.Entry<TableBucket, Long> entry : offsets.entrySet()) {
            long start = entry.getValue();
            long stop = target.get(entry.getKey());
            if (start < 0
                    || start > stop
                    || (start < stop && start < earliest.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
