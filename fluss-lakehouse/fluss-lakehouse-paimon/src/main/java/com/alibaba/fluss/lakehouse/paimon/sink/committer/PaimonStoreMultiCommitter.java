/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.sink.committer;

import com.alibaba.fluss.lakehouse.paimon.sink.committable.FlussLogOffsetCommittable;
import com.alibaba.fluss.lakehouse.paimon.sink.committable.PaimonWrapperManifestCommittable;
import com.alibaba.fluss.metadata.TableBucket;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.LogOffsetCommittable;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.sink.StoreMultiCommitter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/**
 * Most copied from Paimon's {@link StoreMultiCommitter}, but add the logic that after committing to
 * Paimon, it'll also report to Fluss so that Fluss can know what data has been tiered to Paimon.
 */
public class PaimonStoreMultiCommitter
        implements Committer<MultiTableCommittable, PaimonWrapperManifestCommittable> {

    private final Catalog catalog;
    private final Context context;

    // To make the commit behavior consistent with that of Committer,
    //    StoreMultiCommitter manages multiple committers which are
    //    referenced by table id.
    private final Map<Identifier, StoreCommitter> tableCommitters;
    // use to get snapshot id after commit
    private final Map<Identifier, FileStoreTable> tablesById;

    // Currently, only compact_database job needs to ignore empty commit and set dynamic options
    private final boolean ignoreEmptyCommit;
    private final Map<String, String> dynamicOptions;

    private final LakeTableSnapshotCommitter lakeTableSnapshotCommitter;

    // the committed buckets, used to help not to commit
    // start log offset in every commit to Fluss
    private final Set<TableBucket> committedBuckets;

    public PaimonStoreMultiCommitter(
            CatalogLoader catalogLoader,
            Context context,
            LakeTableSnapshotCommitter lakeTableSnapshotCommitter) {
        this(catalogLoader, context, lakeTableSnapshotCommitter, false, Collections.emptyMap());
    }

    public PaimonStoreMultiCommitter(
            CatalogLoader catalogLoader,
            Context context,
            LakeTableSnapshotCommitter lakeTableSnapshotCommitter,
            boolean ignoreEmptyCommit,
            Map<String, String> dynamicOptions) {
        this.catalog = catalogLoader.load();
        this.context = context;
        this.lakeTableSnapshotCommitter = lakeTableSnapshotCommitter;
        this.ignoreEmptyCommit = ignoreEmptyCommit;
        this.dynamicOptions = dynamicOptions;
        this.tableCommitters = new HashMap<>();
        this.tablesById = new HashMap<>();
        this.committedBuckets = new HashSet<>();
    }

    @Override
    public boolean forceCreatingSnapshot() {
        return true;
    }

    @Override
    public PaimonWrapperManifestCommittable combine(
            long checkpointId, long watermark, List<MultiTableCommittable> committables) {
        PaimonWrapperManifestCommittable wrappedManifestCommittable =
                new PaimonWrapperManifestCommittable(checkpointId, watermark);
        return combine(checkpointId, watermark, wrappedManifestCommittable, committables);
    }

    @Override
    public PaimonWrapperManifestCommittable combine(
            long checkpointId,
            long watermark,
            PaimonWrapperManifestCommittable wrappedManifestCommittable,
            List<MultiTableCommittable> committables) {
        for (MultiTableCommittable committable : committables) {
            ManifestCommittable manifestCommittable =
                    wrappedManifestCommittable.computeCommittableIfAbsent(
                            Identifier.create(committable.getDatabase(), committable.getTable()),
                            checkpointId,
                            watermark);

            // whether it's our custom committable
            if (committable.wrappedCommittable() instanceof FlussLogOffsetCommittable) {
                FlussLogOffsetCommittable flussLogOffsetCommittable =
                        (FlussLogOffsetCommittable) committable.wrappedCommittable();

                // put table identifier to id mapping
                wrappedManifestCommittable.putTableIdentifierAndId(
                        Identifier.create(committable.getDatabase(), committable.getTable()),
                        flussLogOffsetCommittable.getTableId());

                wrappedManifestCommittable.putPartitions(
                        flussLogOffsetCommittable.getPartitionNameById());

                // put bucket log end offset
                wrappedManifestCommittable
                        .computeEndLogOffsetIfAbsent(flussLogOffsetCommittable.getTableId())
                        .putAll(flussLogOffsetCommittable.getBucketEndOffsets());

            } else {
                switch (committable.kind()) {
                    case FILE:
                        CommitMessage file = (CommitMessage) committable.wrappedCommittable();
                        manifestCommittable.addFileCommittable(file);
                        break;
                    case LOG_OFFSET:
                        LogOffsetCommittable offset =
                                (LogOffsetCommittable) committable.wrappedCommittable();
                        manifestCommittable.addLogOffset(
                                offset.bucket(),
                                offset.offset(),
                                tableCommitters
                                        .get(
                                                Identifier.create(
                                                        committable.getDatabase(),
                                                        committable.getTable()))
                                        .allowLogOffsetDuplicate());
                        break;
                }
            }
        }
        return wrappedManifestCommittable;
    }

    @Override
    public void commit(List<PaimonWrapperManifestCommittable> committables)
            throws IOException, InterruptedException {

        if (committables.isEmpty()) {
            return;
        }

        // key by table id
        PaimonAndFlussCommittable paimonAndFlussCommittable = toCommittable(committables);
        Map<Identifier, List<ManifestCommittable>> committableMap =
                paimonAndFlussCommittable.paimonManifestCommittable;
        committableMap.keySet().forEach(this::getStoreCommitter);

        long checkpointId = committables.get(0).checkpointId();
        long watermark = committables.get(0).watermark();

        FlussCommittable flussCommittable = paimonAndFlussCommittable.flussCommittable;
        Map<Long, Long> snapshotIdByTableId = new HashMap<>();
        Map<Long, Map<TableBucket, Long>> logStartOffsetByTableId = new HashMap<>();

        for (Map.Entry<Identifier, StoreCommitter> entry : tableCommitters.entrySet()) {
            List<ManifestCommittable> committableList = committableMap.get(entry.getKey());
            StoreCommitter committer = entry.getValue();
            if (committableList != null) {
                committer.commit(committableList);
                putSnapshotIdAndLogStartOffset(
                        entry.getKey(),
                        flussCommittable,
                        snapshotIdByTableId,
                        logStartOffsetByTableId);
            } else {
                // try best to commit empty snapshot, but tableCommitters may not contain all tables
                if (committer.forceCreatingSnapshot()) {
                    ManifestCommittable combine =
                            committer.combine(checkpointId, watermark, Collections.emptyList());
                    committer.commit(Collections.singletonList(combine));
                }
            }
        }

        LakeTableSnapshotInfo lakeTableSnapshotInfo =
                new LakeTableSnapshotInfo(
                        snapshotIdByTableId,
                        logStartOffsetByTableId,
                        flussCommittable.flussLogEndOffsetByTableId);
        lakeTableSnapshotCommitter.commit(lakeTableSnapshotInfo);
    }

    @Override
    public int filterAndCommit(
            List<PaimonWrapperManifestCommittable> globalCommittables, boolean checkAppendFiles)
            throws IOException {
        int result = 0;

        PaimonAndFlussCommittable paimonAndFlussCommittable = toCommittable(globalCommittables);
        FlussCommittable flussCommittable = paimonAndFlussCommittable.flussCommittable;

        Map<Long, Long> snapshotIdByTableId = new HashMap<>();
        Map<Long, Map<TableBucket, Long>> logStartOffsetByTableId = new HashMap<>();
        for (Map.Entry<Identifier, List<ManifestCommittable>> entry :
                paimonAndFlussCommittable.paimonManifestCommittable.entrySet()) {
            result +=
                    getStoreCommitter(entry.getKey())
                            .filterAndCommit(entry.getValue(), checkAppendFiles);

            putSnapshotIdAndLogStartOffset(
                    entry.getKey(), flussCommittable, snapshotIdByTableId, logStartOffsetByTableId);
        }

        LakeTableSnapshotInfo lakeTableSnapshotInfo =
                new LakeTableSnapshotInfo(
                        snapshotIdByTableId,
                        logStartOffsetByTableId,
                        flussCommittable.flussLogEndOffsetByTableId);
        lakeTableSnapshotCommitter.commit(lakeTableSnapshotInfo);
        return result;
    }

    private void putSnapshotIdAndLogStartOffset(
            Identifier tableIdentifier,
            FlussCommittable flussCommittable,
            Map<Long, Long> latestSnapshotIdByTableId,
            Map<Long, Map<TableBucket, Long>> logStartOffsetByTableId)
            throws IOException {
        Long tableId = flussCommittable.tableIdByPaimonIdentifier.get(tableIdentifier);
        // tableId may be null when no writing records to this table
        if (tableId == null) {
            return;
        }

        // first put latest snapshot id
        FileStoreTable fileStoreTable = tablesById.get(tableIdentifier);

        // put snapshot id
        latestSnapshotIdByTableId.put(tableId, fileStoreTable.snapshotManager().latestSnapshotId());

        // then put log start offset for bucket if never put
        Map<TableBucket, Long> logStartOffsetByBucket = new HashMap<>();

        // hack logic:
        // only put log start offset for log table and is not bucket unaware mode
        // todo: find a way like inner table to record it?
        if (fileStoreTable.schema().primaryKeys().isEmpty()
                && fileStoreTable.bucketMode() != BucketMode.BUCKET_UNAWARE) {
            // only need put for the written table buckets
            Set<TableBucket> writtenBuckets =
                    flussCommittable
                            .flussLogEndOffsetByTableId
                            .getOrDefault(tableId, Collections.emptyMap())
                            .keySet();
            for (TableBucket tableBucket : writtenBuckets) {
                if (!committedBuckets.contains(tableBucket)) {
                    // then, we need to put log start offset
                    Long logStartOffset =
                            getLogStartOffset(
                                    tableIdentifier,
                                    fileStoreTable,
                                    tableBucket,
                                    flussCommittable.partitionNameById);
                    if (logStartOffset != null) {
                        logStartOffsetByBucket.put(tableBucket, logStartOffset);
                        committedBuckets.add(tableBucket);
                    }
                }
            }
        }

        if (!logStartOffsetByBucket.isEmpty()) {
            logStartOffsetByTableId.put(tableId, logStartOffsetByBucket);
        }
    }

    private Long getLogStartOffset(
            Identifier identifier,
            FileStoreTable fileStoreTable,
            TableBucket tableBucket,
            Map<Long, String> partitionNameById)
            throws IOException {
        // to find the first snapshot that contains the records of this bucket
        Iterator<Snapshot> snapshotIterator = fileStoreTable.snapshotManager().snapshots();
        while (snapshotIterator.hasNext()) {
            Snapshot snapshot = snapshotIterator.next();
            List<Split> splits =
                    getSplits(
                            identifier,
                            snapshot.id(),
                            fileStoreTable,
                            tableBucket,
                            partitionNameById);

            if (!splits.isEmpty()) {
                DataSplit dataSplit = (DataSplit) splits.get(0);
                List<DataFileMeta> dataFileMetas = dataSplit.dataFiles();
                if (!dataFileMetas.isEmpty()) {
                    SimpleStats binaryTableStats = dataFileMetas.get(0).valueStats();
                    return binaryTableStats
                            .minValues()
                            // get min log start offset， field count - 2 is the index of log offset
                            .getLong(fileStoreTable.schema().fieldNames().size() - 2);
                }
            }
            snapshotIterator.next();
        }
        return null;
    }

    private List<Split> getSplits(
            Identifier identifier,
            long snapshotId,
            FileStoreTable fileStoreTable,
            TableBucket tableBucket,
            Map<Long, String> partitionNameById) {
        InnerTableScan innerTableScan =
                fileStoreTable
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                        String.valueOf(snapshotId)))
                        .newScan()
                        .withBucketFilter(b -> b == tableBucket.getBucket());
        if (tableBucket.getPartitionId() != null) {
            List<String> partitionKeys = fileStoreTable.partitionKeys();
            checkArgument(
                    partitionKeys.size() == 1,
                    String.format(
                            "Paimon table: %s must be with only 1 partition key, but got %s, the partitions keys are: %s.",
                            identifier, partitionKeys.size(), partitionKeys));
            String partitionKey = fileStoreTable.partitionKeys().get(0);
            innerTableScan =
                    innerTableScan.withPartitionFilter(
                            Collections.singletonMap(
                                    partitionKey,
                                    partitionNameById.get(tableBucket.getPartitionId())));
        }
        return innerTableScan.plan().splits();
    }

    private PaimonAndFlussCommittable toCommittable(
            List<PaimonWrapperManifestCommittable> committables) {
        Map<Identifier, List<ManifestCommittable>> paimonManifestCommittable = new HashMap<>();
        Map<Long, Map<TableBucket, Long>> flussLogEndOffsetByTableId = new HashMap<>();
        Map<Identifier, Long> tableIdByPaimonIdentifier = new HashMap<>();
        Map<Long, String> partitionNameById = new HashMap<>();

        for (PaimonWrapperManifestCommittable manifestCommittable : committables) {
            // paimon manifest committable
            for (Map.Entry<Identifier, ManifestCommittable> committableEntry :
                    manifestCommittable.manifestCommittables().entrySet()) {
                paimonManifestCommittable
                        .computeIfAbsent(committableEntry.getKey(), k -> new ArrayList<>())
                        .add(committableEntry.getValue());
            }

            // log end offset to commit to fluss
            for (Map.Entry<Long, Map<TableBucket, Long>> tableEndLogOffsetEntry :
                    manifestCommittable.getTableLogEndOffset().entrySet()) {
                flussLogEndOffsetByTableId.computeIfAbsent(
                        tableEndLogOffsetEntry.getKey(), k -> new HashMap<>());
                flussLogEndOffsetByTableId
                        .get(tableEndLogOffsetEntry.getKey())
                        .putAll(tableEndLogOffsetEntry.getValue());
            }

            partitionNameById.putAll(manifestCommittable.getPartitionNameById());
            tableIdByPaimonIdentifier.putAll(manifestCommittable.getTableIdByIdentifier());
        }

        FlussCommittable flussCommittable =
                new FlussCommittable(
                        tableIdByPaimonIdentifier, partitionNameById, flussLogEndOffsetByTableId);

        return new PaimonAndFlussCommittable(paimonManifestCommittable, flussCommittable);
    }

    @Override
    public Map<Long, List<MultiTableCommittable>> groupByCheckpoint(
            Collection<MultiTableCommittable> committables) {
        Map<Long, List<MultiTableCommittable>> grouped = new HashMap<>();
        for (MultiTableCommittable c : committables) {
            grouped.computeIfAbsent(c.checkpointId(), k -> new ArrayList<>()).add(c);
        }
        return grouped;
    }

    private StoreCommitter getStoreCommitter(Identifier tableId) {
        StoreCommitter committer = tableCommitters.get(tableId);

        if (committer == null) {
            FileStoreTable table;
            try {
                table = (FileStoreTable) catalog.getTable(tableId).copy(dynamicOptions);
            } catch (Catalog.TableNotExistException e) {
                throw new RuntimeException(
                        String.format(
                                "Failed to get committer for table %s", tableId.getFullName()),
                        e);
            }
            committer =
                    new StoreCommitter(
                            table,
                            table.newCommit(context.commitUser())
                                    .ignoreEmptyCommit(ignoreEmptyCommit),
                            context);
            tableCommitters.put(tableId, committer);
            tablesById.put(tableId, table);
        }

        return committer;
    }

    @Override
    public void close() throws Exception {
        for (StoreCommitter committer : tableCommitters.values()) {
            committer.close();
        }
        lakeTableSnapshotCommitter.close();
    }

    private static class PaimonAndFlussCommittable {
        private final Map<Identifier, List<ManifestCommittable>> paimonManifestCommittable;
        private final FlussCommittable flussCommittable;

        public PaimonAndFlussCommittable(
                Map<Identifier, List<ManifestCommittable>> paimonManifestCommittable,
                FlussCommittable flussCommittable) {
            this.paimonManifestCommittable = paimonManifestCommittable;
            this.flussCommittable = flussCommittable;
        }
    }

    private static class FlussCommittable {
        private final Map<Identifier, Long> tableIdByPaimonIdentifier;
        private final Map<Long, String> partitionNameById;
        private final Map<Long, Map<TableBucket, Long>> flussLogEndOffsetByTableId;

        private FlussCommittable(
                Map<Identifier, Long> tableIdByPaimonIdentifier,
                Map<Long, String> partitionNameById,
                Map<Long, Map<TableBucket, Long>> flussLogEndOffsetByTableId) {
            this.tableIdByPaimonIdentifier = tableIdByPaimonIdentifier;
            this.partitionNameById = partitionNameById;
            this.flussLogEndOffsetByTableId = flussLogEndOffsetByTableId;
        }
    }
}
