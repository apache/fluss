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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.rpc.messages.PbUnionReadContext;
import org.apache.fluss.rpc.messages.PbUnionReadLake;
import org.apache.fluss.rpc.messages.PbUnionReadLogRange;
import org.apache.fluss.rpc.util.PredicateMessageUtils;
import org.apache.fluss.utils.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Captures structured bounded UnionRead inputs for client or engine planning.
 *
 * <p>Calls block the caller and must run on a planning thread, not an Admin RPC callback thread.
 * They use the supplied Admin without closing it or opening lake files. Request timeouts follow the
 * client configuration; there is no additional whole-capture timeout. Interrupted waits abort
 * capture, but do not cancel RPCs already issued. Concurrent calls have independent capture state.
 *
 * <p>The result fixes a schema, an optional lake snapshot and per-bucket log ranges; it is neither
 * a cross-bucket transactional snapshot nor a retention lease. Without a lake baseline, PK reads
 * require complete logs beginning at offset zero. KV snapshot fallback and shared historical logs
 * are not yet supported. Consumers must enforce the captured scope on both lake and log inputs and
 * resolve the captured schema ID against the same table ID.
 *
 * <p>The returned generated message belongs to the caller. Treat it as immutable once shared with
 * workers. It contains no credentials or physical file tasks; engines own task distribution.
 *
 * @since 1.1
 */
@PublicEvolving
public final class UnionReadBoundaryPlanner {

    private UnionReadBoundaryPlanner() {}

    /**
     * Captures table-wide boundaries, suitable for sharing across scans with different predicates.
     *
     * @param admin caller-owned Admin client
     * @param tablePath table whose boundaries should be captured
     * @return captured source boundaries in the shared Java/Rust protocol
     * @throws InterruptedException if interrupted while waiting for metadata
     * @throws Exception if metadata capture or boundary validation fails
     */
    public static PbUnionReadContext captureReadContext(Admin admin, TablePath tablePath)
            throws Exception {
        return captureReadContext(admin, tablePath, new PartitionSpec(Collections.emptyMap()));
    }

    /**
     * Captures boundaries within an exact, possibly partial partition spec before querying offsets.
     *
     * <p>The scope is copied on entry. Partitions removed or recreated during capture are excluded
     * by ID; other lookup failures propagate. Later schema additions do not replace the frozen
     * schema. A scoped context must not be reused as a table-wide boundary.
     *
     * @param admin caller-owned Admin client
     * @param tablePath table whose boundaries should be captured
     * @param scope partition keys and metadata-encoded values; an empty spec selects the whole
     *     table
     * @return captured source boundaries in the shared Java/Rust protocol
     * @throws InterruptedException if interrupted while waiting for metadata
     * @throws Exception if the scope is invalid, metadata capture fails or boundaries are
     *     inconsistent
     */
    public static PbUnionReadContext captureReadContext(
            Admin admin, TablePath tablePath, PartitionSpec scope) throws Exception {
        checkNotNull(admin, "admin");
        checkNotNull(tablePath, "tablePath");
        checkNotNull(scope, "scope");
        PartitionSpec capturedScope = new PartitionSpec(new HashMap<>(scope.getSpecMap()));
        TableInfo table = admin.getTableInfo(tablePath).get();
        checkArgument(
                table.getPartitionKeys().containsAll(capturedScope.getSpecMap().keySet())
                        && !capturedScope.getSpecMap().containsValue(null),
                "Scope must reference partition keys with non-null encoded values.");
        validateSupported(table);
        Predicate predicate =
                PredicateBuilder.partition(capturedScope.getSpecMap(), table.getRowType());
        PbUnionReadContext context =
                new PbUnionReadContext()
                        .setVersion(1)
                        .setCoverage(0)
                        .setDatabaseName(tablePath.getDatabaseName())
                        .setTableName(tablePath.getTableName())
                        .setTableId(table.getTableId())
                        .setSchemaId(table.getSchemaId())
                        .setTableBucketCount(table.getNumBuckets())
                        .addAllBucketKeys(table.getBucketKeys())
                        .addAllPartitionKeys(table.getPartitionKeys());
        table.getTableConfig()
                .getMergeEngineType()
                .ifPresent(
                        engine -> context.setMergeEngine(engine.name().toLowerCase(Locale.ROOT)));
        if (predicate != null) {
            context.setPartitionScope(
                    PredicateMessageUtils.toPbPredicate(predicate, table.getRowType()));
        }
        LakeSnapshot snapshot = readableSnapshot(admin, table);
        if (table.getTableConfig().isDataLakeEnabled()) {
            TablePath lakePath = table.getLakeTablePath();
            PbUnionReadLake lake =
                    context.setLake()
                            .setFormat(
                                    table.getTableConfig()
                                            .getDataLakeFormat()
                                            .orElseThrow(
                                                    () ->
                                                            new IllegalArgumentException(
                                                                    "Missing lake format."))
                                            .name()
                                            .toLowerCase(Locale.ROOT))
                            .setDatabaseName(lakePath.getDatabaseName())
                            .setTableName(lakePath.getTableName());
            if (snapshot != null) {
                lake.setSnapshotId(snapshot.getSnapshotId());
            }
        }
        List<PartitionInfo> selected =
                table.isPartitioned()
                        ? admin.listPartitionInfos(tablePath, capturedScope).get()
                        : Collections.singletonList(null);
        selected = new ArrayList<>(selected);
        if (table.isPartitioned()) {
            selected.sort(
                    java.util.Comparator.comparing(PartitionInfo::getPartitionName)
                            .thenComparingLong(PartitionInfo::getPartitionId));
        }
        List<PartitionCapture> captures = new ArrayList<>();
        for (PartitionInfo partition : selected) {
            captures.add(readOffsets(admin, table, partition));
        }
        recheckPartitions(admin, table, capturedScope, captures);
        validateIdentity(table, admin.getTableInfo(tablePath).get());
        Map<TableBucket, Long> seams =
                snapshot == null ? Collections.emptyMap() : snapshot.getTableBucketsOffset();
        if (snapshot != null) {
            checkArgument(snapshot.getSnapshotId() >= 0, "Invalid lake snapshot ID.");
            for (Map.Entry<TableBucket, Long> seam : seams.entrySet()) {
                checkArgument(
                        seam.getKey().getTableId() == table.getTableId()
                                && seam.getValue() != null
                                && seam.getValue() >= 0,
                        "Snapshot contains an invalid bucket seam: %s",
                        seam.getKey());
            }
        }
        for (PartitionCapture capture : captures) {
            int count =
                    capture.partition == null
                            ? table.getNumBuckets()
                            : capture.partition.getBucketCount();
            checkArgument(
                    capture.earliest.size() == count && capture.stops.size() == count,
                    "Offset maps must cover exactly the captured buckets.");
            Map<String, String> values =
                    capture.partition == null
                            ? Collections.emptyMap()
                            : capture.partition.getPartitionSpec().getSpecMap();
            for (int bucketId = 0; bucketId < count; bucketId++) {
                TableBucket bucket =
                        new TableBucket(
                                table.getTableId(),
                                capture.partition == null
                                        ? null
                                        : capture.partition.getPartitionId(),
                                bucketId);
                Long begin = capture.earliest.get(bucket);
                Long end = capture.stops.get(bucket);
                Long seam = seams.get(bucket);
                checkArgument(begin != null && end != null, "Missing log offsets for %s", bucket);
                long start = seam == null ? begin : seam;
                checkArgument(
                        start >= 0 && begin >= 0 && end >= start && end >= begin,
                        "Invalid concrete log offsets for %s",
                        bucket);
                checkArgument(
                        !table.hasPrimaryKey() || seam != null || begin == 0,
                        "PK bucket %s has no baseline and its log begins at %s; KV snapshot fallback is required.",
                        bucket,
                        begin);
                PbUnionReadLogRange range =
                        context.addLogRange()
                                .setBucketId(bucketId)
                                .setBucketCount(count)
                                .setBaseline(seam == null ? 0 : 1)
                                .setStartOffset(start)
                                .setEarliestOffset(begin)
                                .setStopOffset(end);
                if (capture.partition != null) {
                    range.setPartitionId(capture.partition.getPartitionId());
                    for (String key : table.getPartitionKeys()) {
                        range.addPartitionValue(
                                checkNotNull(values.get(key), "Missing partition value: " + key));
                    }
                }
            }
        }
        return context;
    }

    @Nullable
    private static LakeSnapshot readableSnapshot(Admin admin, TableInfo table)
            throws InterruptedException, ExecutionException {
        if (!table.getTableConfig().isDataLakeEnabled()) {
            return null;
        }
        try {
            return admin.getReadableLakeSnapshot(table.getTablePath()).get();
        } catch (ExecutionException e) {
            if (ExceptionUtils.stripExecutionException(e)
                    instanceof LakeTableSnapshotNotExistException) {
                return null;
            }
            throw e;
        }
    }

    private static PartitionCapture readOffsets(
            Admin admin, TableInfo table, @Nullable PartitionInfo partition)
            throws InterruptedException {
        PartitionCapture capture = new PartitionCapture(partition);
        try {
            int count = partition == null ? table.getNumBuckets() : partition.getBucketCount();
            checkArgument(count > 0, "Invalid bucket count: %s", count);
            List<Integer> buckets = IntStream.range(0, count).boxed().collect(Collectors.toList());
            capture.putOffsets(
                    table.getTableId(),
                    offsets(
                            admin,
                            table.getTablePath(),
                            partition,
                            buckets,
                            new OffsetSpec.EarliestSpec()),
                    capture.earliest);
            capture.putOffsets(
                    table.getTableId(),
                    offsets(
                            admin,
                            table.getTablePath(),
                            partition,
                            buckets,
                            new OffsetSpec.LatestSpec()),
                    capture.stops);
        } catch (ExecutionException | RuntimeException e) {
            // A failed lookup may be caused by deletion. Only the ID recheck can prove that.
            capture.error = e;
        }
        return capture;
    }

    private static Map<Integer, Long> offsets(
            Admin admin,
            TablePath path,
            @Nullable PartitionInfo partition,
            List<Integer> buckets,
            OffsetSpec spec)
            throws InterruptedException, ExecutionException {
        return (partition == null
                        ? admin.listOffsets(path, buckets, spec)
                        : admin.listOffsets(path, partition.getPartitionName(), buckets, spec))
                .all()
                .get();
    }

    private static void recheckPartitions(
            Admin admin, TableInfo table, PartitionSpec scope, List<PartitionCapture> captures)
            throws Exception {
        Exception recheckError = null;
        if (table.isPartitioned()) {
            try {
                Set<Long> liveIds =
                        admin.listPartitionInfos(table.getTablePath(), scope).get().stream()
                                .map(PartitionInfo::getPartitionId)
                                .collect(Collectors.toSet());
                // Successful name-based lookups can also cross a same-name recreation.
                captures.removeIf(capture -> !liveIds.contains(capture.partition.getPartitionId()));
            } catch (ExecutionException | RuntimeException e) {
                recheckError = e;
            }
        }
        for (PartitionCapture capture : captures) {
            if (capture.error != null) {
                throw capture.error;
            }
        }
        if (recheckError != null) {
            throw recheckError;
        }
    }

    private static void validateSupported(TableInfo table) {
        checkArgument(
                !table.getTableConfig().isHistoricalPartitionEnabled(),
                "UnionRead boundaries do not support shared historical logs yet.");
    }

    private static void validateIdentity(TableInfo captured, TableInfo live) {
        validateSupported(live);
        checkArgument(
                captured.getTableId() == live.getTableId()
                        && captured.getTablePath().equals(live.getTablePath()),
                "Table identity changed during boundary capture.");
        if (captured.getTableConfig().isDataLakeEnabled()) {
            checkArgument(
                    live.getTableConfig().isDataLakeEnabled()
                            && captured.getLakeTablePath().equals(live.getLakeTablePath())
                            && captured.getTableConfig()
                                    .getDataLakeFormat()
                                    .equals(live.getTableConfig().getDataLakeFormat()),
                    "Lake mapping changed during boundary capture.");
        }
    }

    private static final class PartitionCapture {
        @Nullable private final PartitionInfo partition;
        private final Map<TableBucket, Long> earliest = new HashMap<>();
        private final Map<TableBucket, Long> stops = new HashMap<>();
        @Nullable private Exception error;

        private PartitionCapture(@Nullable PartitionInfo partition) {
            this.partition = partition;
        }

        private void putOffsets(
                long tableId, Map<Integer, Long> offsets, Map<TableBucket, Long> target) {
            offsets.forEach(
                    (bucket, offset) ->
                            target.put(
                                    new TableBucket(
                                            tableId,
                                            partition == null ? null : partition.getPartitionId(),
                                            bucket),
                                    offset));
        }
    }
}
