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

package org.apache.fluss.flink.lake;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.exception.FlussException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.messages.PbLakeDvEntry;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.flink.source.split.LogSplit.NO_STOPPING_OFFSET;
import static org.apache.fluss.metadata.ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR;

/** A generator for lake splits. */
public class LakeSplitGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSplitGenerator.class);

    // Retry budget is intentionally short: DV unavailability during a Readable-Switch is a
    // ~1s transient, so a few quick retries cover it. A longer budget would turn a not-yet-readable
    // table (bootstrap / tiering down) into a multi-minute hang instead of a fast, clear failure.
    private static final int MAX_OUTER_RETRIES = 3;
    private static final int MAX_DV_FETCH_RETRIES = 3;
    private static final long OUTER_BACKOFF_MS = 400;
    private static final long INITIAL_BACKOFF_MS = 300;
    private static final long MAX_BACKOFF_MS = 800;

    private final TableInfo tableInfo;
    private final Admin flussAdmin;
    private final OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final int bucketCount;
    private final Supplier<Set<PartitionInfo>> listPartitionSupplier;
    private final boolean dvEnabled;

    private final LakeSource<LakeSplit> lakeSource;

    public LakeSplitGenerator(
            TableInfo tableInfo,
            Admin flussAdmin,
            LakeSource<LakeSplit> lakeSource,
            OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever,
            OffsetsInitializer stoppingOffsetInitializer,
            int bucketCount,
            Supplier<Set<PartitionInfo>> listPartitionSupplier) {
        this.tableInfo = tableInfo;
        this.flussAdmin = flussAdmin;
        this.lakeSource = lakeSource;
        this.bucketOffsetsRetriever = bucketOffsetsRetriever;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.bucketCount = bucketCount;
        this.listPartitionSupplier = listPartitionSupplier;
        this.dvEnabled = tableInfo.getTableConfig().isDeletionVectorsEnabled();
    }

    /**
     * Return A list of hybrid lake snapshot {@link LakeSnapshotSplit}, {@link
     * LakeSnapshotAndFlussLogSplit} and the corresponding Fluss {@link LogSplit} based on the lake
     * snapshot. Return null if no lake snapshot exists.
     *
     * <p>If DV is enabled, fetches DV snapshots from TabletServers with retry:
     *
     * <ul>
     *   <li>Inner retry: per-bucket backoff for "not ready" (server hasn't completed Switch yet)
     *   <li>Outer retry: refresh LakeSnapshot when snapshot has been superseded
     * </ul>
     */
    @Nullable
    public List<SourceSplitBase> generateHybridLakeFlussSplits() throws Exception {
        // Snapshot id to pin lake planning + DV fetch to. Null means "use the coordinator's
        // readable lake snapshot"; on a stale-snapshot error we re-sync to the id the TabletServer
        // reports as its current readable, so the two readable pointers converge instead of failing
        // the read while tiering advances the ZK pointer during ingestion.
        Long overrideSnapshotId = null;
        for (int outerRetry = 0; outerRetry < MAX_OUTER_RETRIES; outerRetry++) {
            LakeSnapshot lakeSnapshotInfo;
            try {
                lakeSnapshotInfo =
                        (overrideSnapshotId == null
                                        ? flussAdmin.getReadableLakeSnapshot(
                                                tableInfo.getTablePath())
                                        : flussAdmin.getLakeSnapshot(
                                                tableInfo.getTablePath(), overrideSnapshotId))
                                .get();
            } catch (Exception exception) {
                if (ExceptionUtils.stripExecutionException(exception)
                        instanceof LakeTableSnapshotNotExistException) {
                    return null;
                }
                throw exception;
            }

            long snapshotId = lakeSnapshotInfo.getSnapshotId();

            boolean isLogTable = !tableInfo.hasPrimaryKey();
            boolean isPartitioned = tableInfo.isPartitioned();

            Map<String, Map<Integer, List<LakeSplit>>> lakeSplits =
                    groupLakeSplits(
                            lakeSource
                                    .createPlanner((LakeSource.PlannerContext) () -> snapshotId)
                                    .plan());

            Map<TableBucket, Long> tableBucketsOffset = lakeSnapshotInfo.getTableBucketsOffset();

            // Pre-compute stopping offsets and partition info
            Map<Long, String> partitionNameById = null;
            Map<TableBucket, Long> allStoppingOffsets = new HashMap<>();
            List<Integer> bucketIds =
                    IntStream.range(0, bucketCount).boxed().collect(Collectors.toList());

            if (isPartitioned) {
                Set<PartitionInfo> partitionInfos = listPartitionSupplier.get();
                partitionNameById =
                        partitionInfos.stream()
                                .collect(
                                        Collectors.toMap(
                                                PartitionInfo::getPartitionId,
                                                PartitionInfo::getPartitionName));
                for (Map.Entry<Long, String> entry : partitionNameById.entrySet()) {
                    Map<Integer, Long> offsets =
                            stoppingOffsetInitializer.getBucketOffsets(
                                    entry.getValue(), bucketIds, bucketOffsetsRetriever);
                    for (Map.Entry<Integer, Long> offsetEntry : offsets.entrySet()) {
                        allStoppingOffsets.put(
                                new TableBucket(
                                        tableInfo.getTableId(),
                                        entry.getKey(),
                                        offsetEntry.getKey()),
                                offsetEntry.getValue());
                    }
                }
            } else {
                Map<Integer, Long> offsets =
                        stoppingOffsetInitializer.getBucketOffsets(
                                null, bucketIds, bucketOffsetsRetriever);
                for (Map.Entry<Integer, Long> offsetEntry : offsets.entrySet()) {
                    allStoppingOffsets.put(
                            new TableBucket(tableInfo.getTableId(), null, offsetEntry.getKey()),
                            offsetEntry.getValue());
                }
            }

            // Fetch DV data if enabled, only for buckets with data gap
            Map<TableBucket, DvSnapshotInfo> bucketDvSnapshots = null;
            if (dvEnabled) {
                Set<TableBucket> bucketsNeedingDv =
                        findBucketsNeedingDv(tableBucketsOffset, allStoppingOffsets);
                if (!bucketsNeedingDv.isEmpty()) {
                    try {
                        bucketDvSnapshots = fetchDvForAllBuckets(snapshotId, bucketsNeedingDv);
                    } catch (Exception e) {
                        // DV not ready for some bucket: a Readable-Switch is in progress, or a
                        // bucket just went empty->data during ingestion and hasn't completed its
                        // first switch (readableSnapshotId == -1). The error arrives wrapped
                        // (StaleSnapshotException inside a FlussRuntimeException surfaced as an
                        // UnknownServerException), so detect it by message. Re-fetch the
                        // coordinator's readable snapshot and retry; this converges once the
                        // bucket's switch completes. Bounded by MAX_OUTER_RETRIES (the "hold").
                        if (!isDvTransientlyUnavailable(e)) {
                            throw e;
                        }
                        StaleSnapshotException stale =
                                ExceptionUtils.findThrowable(e, StaleSnapshotException.class)
                                        .orElse(null);
                        // Iceberg snapshot ids are random, so a positive server-readable id lets us
                        // re-plan against it directly; otherwise just refresh the coordinator's
                        // readable snapshot on the next round.
                        overrideSnapshotId =
                                (stale != null && stale.getCurrentSnapshotId() > 0)
                                        ? Long.valueOf(stale.getCurrentSnapshotId())
                                        : null;
                        LOG.info(
                                "DV snapshot {} not ready, refreshing readable snapshot and retrying"
                                        + " (outer {}/{}).",
                                snapshotId,
                                outerRetry + 1,
                                MAX_OUTER_RETRIES);
                        Thread.sleep(OUTER_BACKOFF_MS);
                        continue;
                    }
                }
            }

            if (isPartitioned) {
                return generatePartitionTableSplit(
                        lakeSplits,
                        isLogTable,
                        tableBucketsOffset,
                        partitionNameById,
                        allStoppingOffsets,
                        bucketDvSnapshots);
            } else {
                Map<Integer, List<LakeSplit>> nonPartitionLakeSplits =
                        lakeSplits.isEmpty() ? null : lakeSplits.values().iterator().next();
                // non-partitioned table
                return generateNoPartitionedTableSplit(
                        nonPartitionLakeSplits,
                        isLogTable,
                        tableBucketsOffset,
                        allStoppingOffsets,
                        bucketDvSnapshots);
            }
        }
        throw new FlussException(
                "Failed to fetch DV snapshots after "
                        + MAX_OUTER_RETRIES
                        + " retries due to snapshot superseding");
    }

    private Map<String, Map<Integer, List<LakeSplit>>> groupLakeSplits(List<LakeSplit> lakeSplits) {
        Map<String, Map<Integer, List<LakeSplit>>> result = new HashMap<>();
        for (LakeSplit split : lakeSplits) {
            String partition = String.join(PARTITION_SPEC_SEPARATOR, split.partition());
            int bucket = split.bucket();
            // Get or create the partition group
            Map<Integer, List<LakeSplit>> bucketMap =
                    result.computeIfAbsent(partition, k -> new HashMap<>());
            List<LakeSplit> splitList = bucketMap.computeIfAbsent(bucket, k -> new ArrayList<>());
            splitList.add(split);
        }
        return result;
    }

    private List<SourceSplitBase> generatePartitionTableSplit(
            Map<String, Map<Integer, List<LakeSplit>>> lakeSplits,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Long, String> partitionNameById,
            Map<TableBucket, Long> allStoppingOffsets,
            @Nullable Map<TableBucket, DvSnapshotInfo> bucketDvSnapshots) {
        List<SourceSplitBase> splits = new ArrayList<>();
        Map<String, Long> flussPartitionIdByName =
                partitionNameById.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getValue,
                                        Map.Entry::getKey,
                                        (existing, replacement) -> existing,
                                        LinkedHashMap::new));
        long lakeSplitPartitionId = -1L;

        // iterate lake splits
        for (Map.Entry<String, Map<Integer, List<LakeSplit>>> lakeSplitEntry :
                lakeSplits.entrySet()) {
            String partitionName = lakeSplitEntry.getKey();
            Map<Integer, List<LakeSplit>> lakeSplitsOfPartition = lakeSplitEntry.getValue();
            Long partitionId = flussPartitionIdByName.remove(partitionName);
            if (partitionId != null) {
                // mean the partition also exist in fluss partition
                splits.addAll(
                        generateSplit(
                                lakeSplitsOfPartition,
                                partitionId,
                                partitionName,
                                isLogTable,
                                tableBucketSnapshotLogOffset,
                                allStoppingOffsets,
                                bucketDvSnapshots));

            } else {
                // only lake data
                splits.addAll(
                        toLakeSnapshotSplits(
                                lakeSplitsOfPartition,
                                partitionName,
                                // now, we can't get partition id for the partition only
                                // in lake, set them to a arbitrary partition id, but
                                // make sure different partition have different partition id
                                // to enable different partition can be distributed to different
                                // tasks
                                lakeSplitPartitionId--));
            }
        }

        // iterate remain fluss splits
        for (Map.Entry<String, Long> partitionIdByNameEntry : flussPartitionIdByName.entrySet()) {
            String partitionName = partitionIdByNameEntry.getKey();
            Long partitionId = partitionIdByNameEntry.getValue();
            splits.addAll(
                    generateSplit(
                            null,
                            partitionId,
                            partitionName,
                            isLogTable,
                            // pass empty map since we won't read lake splits
                            Collections.emptyMap(),
                            allStoppingOffsets,
                            bucketDvSnapshots));
        }
        return splits;
    }

    private List<SourceSplitBase> generateSplit(
            @Nullable Map<Integer, List<LakeSplit>> lakeSplits,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<TableBucket, Long> allStoppingOffsets,
            @Nullable Map<TableBucket, DvSnapshotInfo> bucketDvSnapshots) {
        List<SourceSplitBase> splits = new ArrayList<>();
        if (isLogTable) {
            if (lakeSplits != null) {
                splits.addAll(toLakeSnapshotSplits(lakeSplits, partitionName, partitionId));
            }
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                Long stoppingOffset = allStoppingOffsets.get(tableBucket);
                if (stoppingOffset == null) {
                    stoppingOffset = NO_STOPPING_OFFSET;
                }
                if (snapshotLogOffset == null) {
                    // no data committed to lake for this bucket, scan from fluss log
                    if (stoppingOffset == NO_STOPPING_OFFSET || stoppingOffset > 0) {
                        splits.add(
                                new LogSplit(
                                        tableBucket,
                                        partitionName,
                                        EARLIEST_OFFSET,
                                        stoppingOffset));
                    }
                } else {
                    // need to read remain fluss log
                    if (stoppingOffset == NO_STOPPING_OFFSET
                            || snapshotLogOffset < stoppingOffset) {
                        splits.add(
                                new LogSplit(
                                        tableBucket,
                                        partitionName,
                                        snapshotLogOffset,
                                        stoppingOffset));
                    }
                }
            }
        } else {
            // it's primary key table
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                Long stoppingOffset = allStoppingOffsets.get(tableBucket);
                if (stoppingOffset == null) {
                    stoppingOffset = NO_STOPPING_OFFSET;
                }
                DvSnapshotInfo dvSnapshot =
                        bucketDvSnapshots != null ? bucketDvSnapshots.get(tableBucket) : null;
                splits.addAll(
                        generateSplitForPrimaryKeyTableBucket(
                                lakeSplits != null ? lakeSplits.get(bucket) : null,
                                tableBucket,
                                partitionName,
                                snapshotLogOffset,
                                stoppingOffset,
                                dvSnapshot));
            }
        }

        return splits;
    }

    private List<SourceSplitBase> toLakeSnapshotSplits(
            Map<Integer, List<LakeSplit>> lakeSplits,
            @Nullable String partitionName,
            @Nullable Long partitionId) {
        List<SourceSplitBase> splits = new ArrayList<>();
        // we may have multiple table buckets; so we need to
        // introduce an index to make split unique
        int index = 0;
        for (LakeSplit lakeSplit :
                lakeSplits.values().stream().flatMap(List::stream).collect(Collectors.toList())) {
            TableBucket tableBucket =
                    new TableBucket(tableInfo.getTableId(), partitionId, lakeSplit.bucket());
            splits.add(new LakeSnapshotSplit(tableBucket, partitionName, lakeSplit, index++));
        }
        return splits;
    }

    private List<SourceSplitBase> generateSplitForPrimaryKeyTableBucket(
            @Nullable List<LakeSplit> lakeSplits,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable Long snapshotLogOffset,
            long stoppingOffset,
            @Nullable DvSnapshotInfo dvSnapshot) {
        // DV-enabled tables (Paimon or Iceberg) must never use the sort-merge
        // LakeSnapshotAndFlussLogSplit path: Iceberg's reader is not a SortedRecordReader and would
        // NPE. Always emit DV-style splits (LakeSnapshotSplit + LogSplit); use empty DV maps when
        // DV data is unavailable so the read still works.
        if (dvEnabled) {
            return generateDvSplitForBucket(
                    lakeSplits,
                    tableBucket,
                    partitionName,
                    snapshotLogOffset,
                    stoppingOffset,
                    dvSnapshot);
        }

        // no snapshot data for this bucket or no a corresponding log offset in this bucket,
        // can only scan from change log
        if (snapshotLogOffset == null || snapshotLogOffset < 0) {
            return Collections.singletonList(
                    new LakeSnapshotAndFlussLogSplit(
                            tableBucket, partitionName, null, EARLIEST_OFFSET, stoppingOffset));
        }

        // No DV available: fall back to sort-merge via LakeSnapshotAndFlussLogSplit
        if (dvSnapshot == null) {
            return Collections.singletonList(
                    new LakeSnapshotAndFlussLogSplit(
                            tableBucket,
                            partitionName,
                            lakeSplits,
                            snapshotLogOffset,
                            stoppingOffset,
                            0,
                            0,
                            lakeSplits == null,
                            null));
        }

        // DV available: split into independent lake + log splits with DV filtering
        LOG.info(
                "Using DV-based split for bucket {}: lakeDvSize={}, logDvPresent={}, "
                        + "snapshotLogOffset={}, stoppingOffset={}, dvLogEndOffset={}",
                tableBucket,
                dvSnapshot.getLakeDv().size(),
                dvSnapshot.getLogDvBitmap() != null,
                snapshotLogOffset,
                stoppingOffset,
                dvSnapshot.getLogEndOffset());
        List<SourceSplitBase> splits = new ArrayList<>();

        // Truncate stoppingOffset to DV coverage range
        if (stoppingOffset > 0) {
            stoppingOffset = Math.min(stoppingOffset, dvSnapshot.getLogEndOffset());
        }

        // Generate LakeSnapshotSplit(s) with lakeDv map
        if (lakeSplits != null) {
            Map<String, byte[]> lakeDvMap = dvSnapshot.getLakeDv();
            int index = 0;
            for (LakeSplit lakeSplit : lakeSplits) {
                splits.add(
                        new LakeSnapshotSplit(
                                tableBucket, partitionName, lakeSplit, index++, 0, lakeDvMap));
            }
        }

        // Generate LogSplit with logDv bitmap.
        // Use empty byte[] when logDvBitmap is null to indicate DV batch read mode
        // (enables DELETE/UPDATE_BEFORE filtering even when no specific offsets to skip).
        if (stoppingOffset == NO_STOPPING_OFFSET || snapshotLogOffset < stoppingOffset) {
            byte[] logDvBitmap = dvSnapshot.getLogDvBitmap();
            if (logDvBitmap == null) {
                logDvBitmap = new byte[0];
            }
            splits.add(
                    new LogSplit(
                            tableBucket,
                            partitionName,
                            snapshotLogOffset,
                            stoppingOffset,
                            logDvBitmap));
        }

        return splits;
    }

    /**
     * Builds DV-style splits for one PK-table bucket: a {@link LakeSnapshotSplit} per tiered file
     * (masked by lakeDv) plus a {@link LogSplit} for the hot tail (DV batch read mode). Used for
     * all DV-enabled tables so we never take the sort-merge path. When {@code dvSnapshot} is null
     * (DV momentarily unavailable / bucket not requiring DV), empty DV maps are used so the read
     * still returns rows instead of failing.
     */
    private List<SourceSplitBase> generateDvSplitForBucket(
            @Nullable List<LakeSplit> lakeSplits,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable Long snapshotLogOffset,
            long stoppingOffset,
            @Nullable DvSnapshotInfo dvSnapshot) {
        List<SourceSplitBase> splits = new ArrayList<>();
        Map<String, byte[]> lakeDvMap =
                dvSnapshot != null ? dvSnapshot.getLakeDv() : Collections.emptyMap();
        boolean hasTieredData = snapshotLogOffset != null && snapshotLogOffset >= 0;

        // Lake side: one LakeSnapshotSplit per tiered file (masked by lakeDv). Skipped when this
        // bucket has no tiered data in the readable snapshot (all its rows are still in the log).
        if (hasTieredData && lakeSplits != null) {
            int index = 0;
            for (LakeSplit lakeSplit : lakeSplits) {
                splits.add(
                        new LakeSnapshotSplit(
                                tableBucket, partitionName, lakeSplit, index++, 0, lakeDvMap));
            }
        }

        // Log side: read the hot tail from the tiered offset, or EARLIEST when nothing is tiered
        // for this bucket yet. Truncate to the DV coverage end when known. An (empty) logDvBitmap
        // enables DV batch read mode (filters DELETE/UPDATE_BEFORE).
        long logStart = hasTieredData ? snapshotLogOffset : EARLIEST_OFFSET;
        long logStop = stoppingOffset;
        if (dvSnapshot != null && logStop > 0) {
            logStop = Math.min(logStop, dvSnapshot.getLogEndOffset());
        }
        // An empty bucket (no tiered data, no lake files, and no bounded log to read) contributes
        // nothing to a batch union read. Emitting a log split for it would subscribe the reader to
        // a bucket with no records, and an empty bounded log split never reaches its record-driven
        // stop condition, so the query would hang. Skip it.
        boolean bucketIsEmpty =
                !hasTieredData
                        && (lakeSplits == null || lakeSplits.isEmpty())
                        && (logStop == NO_STOPPING_OFFSET || logStop <= 0);
        if (!bucketIsEmpty
                && (logStop == NO_STOPPING_OFFSET || !hasTieredData || logStart < logStop)) {
            byte[] logDvBitmap = dvSnapshot != null ? dvSnapshot.getLogDvBitmap() : null;
            if (logDvBitmap == null) {
                logDvBitmap = new byte[0];
            }
            splits.add(new LogSplit(tableBucket, partitionName, logStart, logStop, logDvBitmap));
        }
        return splits;
    }

    private List<SourceSplitBase> generateNoPartitionedTableSplit(
            @Nullable Map<Integer, List<LakeSplit>> lakeSplits,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<TableBucket, Long> allStoppingOffsets,
            @Nullable Map<TableBucket, DvSnapshotInfo> bucketDvSnapshots) {
        return generateSplit(
                lakeSplits,
                null,
                null,
                isLogTable,
                tableBucketSnapshotLogOffset,
                allStoppingOffsets,
                bucketDvSnapshots);
    }

    // --------- DV fetch helpers ---------

    /**
     * Finds buckets that need DV data. A DV-enabled bucket needs its DV snapshot when either it has
     * lake snapshot data (whose stale rows must be masked by LakeDv) or there is a hot-log gap
     * (readable offset &lt; latest, needing LogDv). Buckets with lake data but no log gap must
     * still be included; otherwise they fall back to the sort-merge path, which has no comparator
     * for Iceberg (non-sorted lake reader) and throws.
     */
    private Set<TableBucket> findBucketsNeedingDv(
            Map<TableBucket, Long> tableBucketsOffset, Map<TableBucket, Long> allStoppingOffsets) {
        Set<TableBucket> result = new HashSet<>();
        for (Map.Entry<TableBucket, Long> entry : tableBucketsOffset.entrySet()) {
            TableBucket tb = entry.getKey();
            long snapshotLogOffset = entry.getValue();
            // Only buckets with tiered lake data need a DV fetch (LakeDv for their files + LogDv
            // for the hot tail). A bucket with no tiered data (snapshotLogOffset < 0) has no lake
            // files to mask and, being empty, may never have gone through a Readable-Switch
            // (readableSnapshotId == -1) -> its DV fetch would fail and take down the whole read.
            // Such buckets are handled by generateDvSplitForBucket (log-only / skipped) without a
            // DV fetch.
            if (snapshotLogOffset >= 0) {
                result.add(tb);
            }
        }
        return result;
    }

    /**
     * Fetches DV snapshot for the specified table buckets. Per-bucket retry for "not ready" errors.
     * Throws {@link StaleSnapshotException} (superseded) to caller for outer retry.
     */
    private Map<TableBucket, DvSnapshotInfo> fetchDvForAllBuckets(
            long snapshotId, Set<TableBucket> tableBuckets) throws Exception {
        TablePath tablePath = tableInfo.getTablePath();
        Map<TableBucket, DvSnapshotInfo> results = new HashMap<>();
        for (TableBucket tb : tableBuckets) {
            DvSnapshotInfo dv = fetchDvForBucketWithRetry(tablePath, tb, snapshotId);
            // null => this bucket has no readable DV yet; it falls back to the no-DV split path
            // (lake + log, -D/-U filtered) rather than failing or holding the whole read.
            if (dv != null) {
                results.put(tb, dv);
            }
        }
        return results;
    }

    @Nullable
    private DvSnapshotInfo fetchDvForBucketWithRetry(
            TablePath tablePath, TableBucket tableBucket, long snapshotId) throws Exception {
        long backoffMs = INITIAL_BACKOFF_MS;
        Exception last = null;
        for (int attempt = 0; attempt < MAX_DV_FETCH_RETRIES; attempt++) {
            try {
                GetDvSnapshotResponse resp =
                        flussAdmin
                                .getDvSnapshot(
                                        tablePath,
                                        tableBucket.getTableId(),
                                        tableBucket.getPartitionId(),
                                        tableBucket.getBucket(),
                                        snapshotId)
                                .get();
                return toDvSnapshotInfo(resp);
            } catch (Exception e) {
                last = e;
                if (!isDvTransientlyUnavailable(e)) {
                    throw e;
                }
                if (isDvNotEstablished(e)) {
                    // current == -1: this bucket has no readable DV snapshot yet (newly populated /
                    // bootstrapping). Its lake data has no server-side DV. Rather than hold or
                    // fail, read it WITHOUT DV (empty maps) via the DV split path -- Iceberg cannot
                    // sort-merge, but generateDvSplitForBucket emits plain lake + log splits with
                    // -D/-U filtered. Correct for the normal insert case; a key updated within the
                    // bucket's very first tiered batch may show twice for ~one round until its DV
                    // materializes, then self-corrects. This keeps the read instant.
                    LOG.info(
                            "Bucket {} has no readable DV snapshot yet (current -1); "
                                    + "reading without DV.",
                            tableBucket);
                    return null;
                }
                // current > 0: the server's readable is a different valid snapshot (a
                // Readable-Switch just moved it). Brief backoff; the outer loop re-fetches the
                // readable snapshot and re-plans against it.
                LOG.info(
                        "DV snapshot not ready for bucket {} (snapshot {}), retry {}/{}.",
                        tableBucket,
                        snapshotId,
                        attempt + 1,
                        MAX_DV_FETCH_RETRIES);
                Thread.sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            }
        }
        throw last;
    }

    /**
     * Whether the DV-unavailable error is specifically the "no readable snapshot yet" (current -1)
     * case, as opposed to a moved-but-valid readable snapshot (current > 0).
     */
    private static boolean isDvNotEstablished(Throwable e) {
        Throwable t = e;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && msg.contains("current -1")) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /**
     * Whether the exception indicates a transiently unavailable DV snapshot (Readable-Switch in
     * progress or bootstrap). Handles both a direct {@link StaleSnapshotException} and the wrapped
     * form the RPC delivers (message contains the server's DV-fetch or stale-snapshot text).
     */
    private static boolean isDvTransientlyUnavailable(Throwable e) {
        if (ExceptionUtils.findThrowable(e, StaleSnapshotException.class).isPresent()) {
            return true;
        }
        Throwable t = e;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null
                    && (msg.contains("Stale snapshot")
                            || msg.contains("Failed to get DV snapshot"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private static DvSnapshotInfo toDvSnapshotInfo(GetDvSnapshotResponse resp) {
        Map<String, byte[]> lakeDv = new HashMap<>();
        for (int i = 0; i < resp.getLakeDvEntriesCount(); i++) {
            PbLakeDvEntry entry = resp.getLakeDvEntryAt(i);
            lakeDv.put(entry.getFilePath(), entry.getDeletedPositionsBitmap());
        }
        byte[] logDvBitmap = resp.hasLogDvBitmap() ? resp.getLogDvBitmap() : null;
        return new DvSnapshotInfo(
                lakeDv, logDvBitmap, resp.getLogEndOffset(), resp.getSnapshotStartOffset());
    }
}
