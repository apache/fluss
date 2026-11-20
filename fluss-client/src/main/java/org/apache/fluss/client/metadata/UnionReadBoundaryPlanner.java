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
import org.apache.fluss.exception.UnionReadDataUnavailableException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.And;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.PartitionPredicateVisitor;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.rpc.messages.PbUnionReadContext;
import org.apache.fluss.rpc.messages.PbUnionReadLake;
import org.apache.fluss.rpc.messages.PbUnionReadLogRange;
import org.apache.fluss.rpc.util.PredicateMessageUtils;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.PartitionUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
 * The Admin must support concurrent requests; metadata capture uses at most eight planning threads
 * per call.
 *
 * <p>The result fixes a schema, an optional lake snapshot and per-bucket log ranges; it is neither
 * a cross-bucket transactional snapshot nor a retention lease. PK buckets use a lake baseline,
 * otherwise a persisted KV snapshot, otherwise complete logs beginning at offset zero. Shared
 * historical logs are not yet supported. Consumers must enforce the captured scope on both lake and
 * log inputs and resolve the captured schema ID against the same table ID.
 *
 * <p>The returned generated message belongs to the caller. Treat it as immutable once shared with
 * workers. It contains no credentials or physical file tasks; engines own task distribution.
 *
 * @since 1.1
 */
@PublicEvolving
public final class UnionReadBoundaryPlanner {

    private static final int CONTEXT_VERSION = 1;
    private static final int COVERAGE_COMPLETE_PARTITIONS = 0;
    private static final int COVERAGE_SINGLE_BUCKET = 1;
    private static final int COVERAGE_LAKE_ONLY = 2;
    private static final int BASELINE_NONE = 0;
    private static final int BASELINE_LAKE = 1;
    private static final int BASELINE_KV = 2;
    private static final int CAPTURE_CONCURRENCY = 8;

    private UnionReadBoundaryPlanner() {}

    /**
     * Captures table-wide boundaries, suitable for sharing across scans with different predicates.
     *
     * @param admin caller-owned Admin client
     * @param tablePath table whose boundaries should be captured
     * @return captured source boundaries in the shared Java/Rust protocol
     * @throws InterruptedException if interrupted while waiting for metadata
     * @throws UnionReadDataUnavailableException if source identity changes during capture or a PK
     *     bucket has neither a baseline nor complete logs
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
     * @throws UnionReadDataUnavailableException if source identity changes during capture or a PK
     *     bucket has neither a baseline nor complete logs
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
        PredicateBuilder builder = new PredicateBuilder(table.getRowType());
        List<Predicate> predicates = new ArrayList<>();
        RowType rowType = table.getRowType();
        for (Map.Entry<String, String> entry : capturedScope.getSpecMap().entrySet()) {
            int index = rowType.getFieldIndex(entry.getKey());
            predicates.add(
                    builder.equal(
                            index,
                            PartitionUtils.parseValueOfType(
                                    entry.getValue(), rowType.getTypeAt(index).getTypeRoot())));
        }
        return captureReadContext(
                admin,
                table,
                capturedScope,
                predicates.isEmpty() ? null : PredicateBuilder.and(predicates));
    }

    /**
     * Captures one shared lake snapshot and the log ranges of partitions matching a predicate.
     *
     * @param admin caller-owned Admin client
     * @param tablePath table whose boundaries should be captured
     * @param scope predicate built against the table schema, referencing only partition columns
     * @return boundaries restricted to the matching partitions
     * @throws InterruptedException if interrupted while waiting for metadata
     * @throws UnionReadDataUnavailableException if source identity changes during capture or a PK
     *     bucket has neither a baseline nor complete logs
     * @throws Exception if capture fails or the predicate is invalid
     */
    public static PbUnionReadContext captureReadContext(
            Admin admin, TablePath tablePath, Predicate scope) throws Exception {
        checkNotNull(scope, "scope");
        TableInfo table = admin.getTableInfo(tablePath).get();
        validateSupported(table);
        checkArgument(
                scope.visit(new PartitionPredicateVisitor(table.getPartitionKeys())),
                "Scope must reference only partition keys.");
        return captureReadContext(admin, table, new PartitionSpec(Collections.emptyMap()), scope);
    }

    /**
     * Selects one bucket without recapturing its boundaries. The task owns its baseline and tail.
     *
     * @param context captured context, treated as immutable
     * @param bucket stable table, partition and bucket identity to select
     * @return an independent context with single-bucket coverage
     * @throws IllegalArgumentException if the bucket is outside the captured context
     */
    public static PbUnionReadContext forBucket(PbUnionReadContext context, TableBucket bucket) {
        checkArgument(context.getTableId() == bucket.getTableId(), "Table ID does not match.");
        for (PbUnionReadLogRange range : context.getLogRangesList()) {
            if (range.getBucketId() == bucket.getBucket()
                    && Objects.equals(
                            range.hasPartitionId() ? range.getPartitionId() : null,
                            bucket.getPartitionId())) {
                PbUnionReadContext selected =
                        copySharedMetadata(context).setCoverage(COVERAGE_SINGLE_BUCKET);
                selected.addLogRange().copyFrom(range);
                return selected;
            }
        }
        throw new IllegalArgumentException("Bucket is outside the captured boundary: " + bucket);
    }

    /**
     * Selects lake partitions not owned by the captured bucket tasks, without querying metadata.
     *
     * <p>Execute this context alongside all {@link #forBucket} tasks to cover the original scope.
     * It has no log ranges, so it must be planned as a lake-only read. The lake planner may divide
     * this scope into multiple file tasks. No matching files is a valid empty result.
     *
     * @param context complete captured context, treated as immutable
     * @param schema the captured schema, from {@link Admin#getTableSchema(TablePath, int)} with the
     *     context's schema ID
     * @return an independent lake-only context, or empty when no readable lake snapshot exists or
     *     the unpartitioned table is already owned by bucket tasks
     * @throws IllegalArgumentException if the context is not complete or the schema ID does not
     *     match
     */
    public static Optional<PbUnionReadContext> forRemainingLakePartitions(
            PbUnionReadContext context, SchemaInfo schema) {
        checkArgument(
                context.hasCoverage() && context.getCoverage() == COVERAGE_COMPLETE_PARTITIONS,
                "Remaining lake partitions require a complete captured context.");
        checkArgument(
                context.getSchemaId() == schema.getSchemaId(),
                "Schema ID must match the captured context.");
        if (!context.hasLake()
                || !context.getLake().hasSnapshotId()
                || (context.getPartitionKeysCount() == 0 && context.getLogRangesCount() > 0)) {
            return Optional.empty();
        }
        RowType rowType = schema.getSchema().getRowType();
        PredicateBuilder builder = new PredicateBuilder(rowType);
        List<Predicate> scopes = new ArrayList<>();
        if (context.hasPartitionScope()) {
            scopes.add(PredicateMessageUtils.toPredicate(context.getPartitionScope(), rowType));
        }
        Set<List<String>> assigned = new LinkedHashSet<>();
        for (PbUnionReadLogRange range : context.getLogRangesList()) {
            assigned.add(range.getPartitionValuesList());
        }
        for (List<String> values : assigned) {
            List<Predicate> differentKeys = new ArrayList<>();
            for (int i = 0; i < context.getPartitionKeysCount(); i++) {
                int index = rowType.getFieldIndex(context.getPartitionKeyAt(i));
                differentKeys.add(
                        builder.notEqual(
                                index,
                                PartitionUtils.parseValueOfType(
                                        values.get(i), rowType.getTypeAt(index).getTypeRoot())));
            }
            // NOT (k1 = v1 AND k2 = v2) keeps the other business partitions.
            scopes.add(PredicateBuilder.or(differentKeys));
        }
        PbUnionReadContext remaining = copySharedMetadata(context).setCoverage(COVERAGE_LAKE_ONLY);
        if (!scopes.isEmpty()) {
            remaining.setPartitionScope(
                    PredicateMessageUtils.toPbPredicate(
                            new CompoundPredicate(And.INSTANCE, scopes), rowType));
        }
        return Optional.of(remaining);
    }

    private static PbUnionReadContext copySharedMetadata(PbUnionReadContext context) {
        PbUnionReadContext selected =
                new PbUnionReadContext()
                        .setVersion(context.getVersion())
                        .setDatabaseName(context.getDatabaseName())
                        .setTableName(context.getTableName())
                        .setTableId(context.getTableId())
                        .setSchemaId(context.getSchemaId())
                        .setTableBucketCount(context.getTableBucketCount())
                        .addAllBucketKeys(context.getBucketKeysList())
                        .addAllPartitionKeys(context.getPartitionKeysList());
        if (context.hasMergeEngine()) {
            selected.setMergeEngine(context.getMergeEngine());
        }
        if (context.hasLake()) {
            selected.setLake().copyFrom(context.getLake());
        }
        if (context.hasPartitionScope()) {
            selected.setPartitionScope().copyFrom(context.getPartitionScope());
        }
        return selected;
    }

    private static PbUnionReadContext captureReadContext(
            Admin admin, TableInfo table, PartitionSpec listingScope, @Nullable Predicate predicate)
            throws Exception {
        TablePath tablePath = table.getTablePath();
        PbUnionReadContext context =
                new PbUnionReadContext()
                        .setVersion(CONTEXT_VERSION)
                        .setCoverage(COVERAGE_COMPLETE_PARTITIONS)
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
                        ? admin.listPartitionInfos(tablePath, listingScope).get()
                        : Collections.singletonList(null);
        selected = new ArrayList<>(selected);
        if (table.isPartitioned()) {
            // Admin already applies exact specs; general predicates need local evaluation.
            if (predicate != null && listingScope.getSpecMap().isEmpty()) {
                RowType partitionType = PartitionUtils.partitionRowType(table);
                int[] mapping =
                        table.getRowType().getFieldNames().stream()
                                .mapToInt(table.getPartitionKeys()::indexOf)
                                .toArray();
                Predicate partitionPredicate =
                        PredicateBuilder.transformFieldMapping(predicate, mapping)
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        "Scope must reference only partition keys."));
                selected.removeIf(
                        partition -> {
                            Map<String, String> values = partition.getPartitionSpec().getSpecMap();
                            return !partitionPredicate.test(
                                    PartitionUtils.toPartitionRow(
                                            table.getPartitionKeys().stream()
                                                    .map(values::get)
                                                    .collect(Collectors.toList()),
                                            partitionType));
                        });
            }
            selected.sort(
                    Comparator.comparing(PartitionInfo::getPartitionName)
                            .thenComparingLong(PartitionInfo::getPartitionId));
        }
        Map<TableBucket, Long> seams =
                snapshot == null ? Collections.emptyMap() : snapshot.getTableBucketsOffset();
        if (snapshot != null) {
            checkArgument(snapshot.getSnapshotId() >= 0, "Invalid lake snapshot ID.");
            for (Map.Entry<TableBucket, Long> seam : seams.entrySet()) {
                if (seam.getKey().getTableId() != table.getTableId()) {
                    throw new UnionReadDataUnavailableException(
                            "Lake snapshot table identity changed during boundary capture.");
                }
                checkArgument(
                        seam.getValue() != null && seam.getValue() >= 0,
                        "Snapshot contains an invalid bucket seam: %s",
                        seam.getKey());
            }
        }
        List<PartitionCapture> captures = capturePartitions(admin, table, selected, seams);
        checkInterrupted();
        recheckPartitions(admin, table, listingScope, captures);
        validateIdentity(table, admin.getTableInfo(tablePath).get());
        for (PartitionCapture capture : captures) {
            int count =
                    PartitionInfo.bucketCountOrDefault(capture.partition, table.getNumBuckets());
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
                Long begin = capture.earliest.get(bucketId);
                Long end = capture.stops.get(bucketId);
                Long seam = seams.get(bucket);
                checkArgument(begin != null && end != null, "Missing log offsets for %s", bucket);
                OptionalLong kvId =
                        capture.kvSnapshots == null
                                ? OptionalLong.empty()
                                : capture.kvSnapshots.getSnapshotId(bucketId);
                int baseline =
                        seam != null
                                ? BASELINE_LAKE
                                : kvId.isPresent() ? BASELINE_KV : BASELINE_NONE;
                long start =
                        seam != null
                                ? seam
                                : kvId.isPresent()
                                        ? capture.kvSnapshots.getLogOffset(bucketId).getAsLong()
                                        : begin;
                checkArgument(
                        start >= 0 && begin >= 0 && end >= start && end >= begin,
                        "Invalid concrete log offsets for %s",
                        bucket);
                if (table.hasPrimaryKey() && baseline == BASELINE_NONE && begin != 0) {
                    throw new UnionReadDataUnavailableException(
                            "PK bucket "
                                    + bucket
                                    + " has no lake or KV snapshot baseline and its log begins at "
                                    + begin);
                }
                PbUnionReadLogRange range =
                        context.addLogRange()
                                .setBucketId(bucketId)
                                .setBucketCount(count)
                                .setBaseline(baseline)
                                .setStartOffset(start)
                                .setEarliestOffset(begin)
                                .setStopOffset(end);
                if (baseline == BASELINE_KV) {
                    range.setKvSnapshotId(kvId.getAsLong());
                }
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

    private static List<PartitionCapture> capturePartitions(
            Admin admin,
            TableInfo table,
            List<PartitionInfo> partitions,
            Map<TableBucket, Long> seams)
            throws InterruptedException, ExecutionException {
        List<PartitionCapture> captures = new ArrayList<>();
        if (partitions.size() <= 1) {
            for (PartitionInfo partition : partitions) {
                captures.add(readOffsets(admin, table, partition, seams));
            }
            return captures;
        }
        // listOffsets refreshes metadata synchronously. Keep it off RPC completion threads.
        ExecutorService executor =
                Executors.newFixedThreadPool(
                        Math.min(CAPTURE_CONCURRENCY, partitions.size()),
                        new ExecutorThreadFactory("union-read-boundary"));
        try {
            List<Future<PartitionCapture>> pending = new ArrayList<>(partitions.size());
            for (PartitionInfo partition : partitions) {
                checkInterrupted();
                pending.add(executor.submit(() -> readOffsets(admin, table, partition, seams)));
            }
            for (Future<PartitionCapture> result : pending) {
                captures.add(result.get());
            }
        } finally {
            executor.shutdownNow();
        }
        return captures;
    }

    private static PartitionCapture readOffsets(
            Admin admin,
            TableInfo table,
            @Nullable PartitionInfo partition,
            Map<TableBucket, Long> seams)
            throws InterruptedException {
        checkInterrupted();
        PartitionCapture capture = new PartitionCapture(partition);
        try {
            int count = PartitionInfo.bucketCountOrDefault(partition, table.getNumBuckets());
            checkArgument(count > 0, "Invalid bucket count: %s", count);
            List<Integer> buckets = IntStream.range(0, count).boxed().collect(Collectors.toList());
            Long partitionId = partition == null ? null : partition.getPartitionId();
            if (table.hasPrimaryKey()
                    && buckets.stream()
                            .anyMatch(
                                    bucket ->
                                            !seams.containsKey(
                                                    new TableBucket(
                                                            table.getTableId(),
                                                            partitionId,
                                                            bucket)))) {
                // Select the snapshot before capturing the stop, so its seam cannot be newer.
                capture.kvSnapshots =
                        (partition == null
                                        ? admin.getLatestKvSnapshots(table.getTablePath())
                                        : admin.getLatestKvSnapshots(
                                                table.getTablePath(), partition.getPartitionName()))
                                .get();
                KvSnapshots snapshots = capture.kvSnapshots;
                if (snapshots.getTableId() != table.getTableId()
                        || !Objects.equals(snapshots.getPartitionId(), partitionId)) {
                    throw new UnionReadDataUnavailableException(
                            "KV snapshot identity changed during boundary capture.");
                }
                checkArgument(
                        snapshots.getBucketIds().stream()
                                .allMatch(bucket -> bucket >= 0 && bucket < count),
                        "Invalid KV snapshot bucket.");
                for (int bucket : buckets) {
                    OptionalLong id = snapshots.getSnapshotId(bucket);
                    OptionalLong offset = snapshots.getLogOffset(bucket);
                    checkArgument(
                            id.isPresent() == offset.isPresent()
                                    && (!id.isPresent()
                                            || (id.getAsLong() >= 0 && offset.getAsLong() >= 0)),
                            "Invalid KV snapshot ID or log seam for bucket %s",
                            bucket);
                }
            }
            CompletableFuture<Map<Integer, Long>> earliest =
                    offsets(
                            admin,
                            table.getTablePath(),
                            partition,
                            buckets,
                            new OffsetSpec.EarliestSpec());
            CompletableFuture<Map<Integer, Long>> latest =
                    offsets(
                            admin,
                            table.getTablePath(),
                            partition,
                            buckets,
                            new OffsetSpec.LatestSpec());
            capture.earliest = earliest.get();
            capture.stops = latest.get();
            checkInterrupted();
        } catch (ExecutionException | RuntimeException e) {
            // A failed lookup may be caused by deletion. Only the ID recheck can prove that.
            capture.error = e;
        }
        return capture;
    }

    private static CompletableFuture<Map<Integer, Long>> offsets(
            Admin admin,
            TablePath path,
            @Nullable PartitionInfo partition,
            List<Integer> buckets,
            OffsetSpec spec)
            throws InterruptedException {
        // Admin's synchronous refresh may finish after capture was interrupted.
        checkInterrupted();
        return (partition == null
                        ? admin.listOffsets(path, buckets, spec)
                        : admin.listOffsets(path, partition.getPartitionName(), buckets, spec))
                .all();
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException("UnionRead boundary capture interrupted.");
        }
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
                table.getNumBuckets() > 0, "Invalid table bucket count: %s", table.getNumBuckets());
        checkArgument(
                !table.getTableConfig().isHistoricalPartitionEnabled(),
                "UnionRead boundaries do not support shared historical logs yet.");
    }

    private static void validateIdentity(TableInfo captured, TableInfo live) {
        if (captured.getTableId() != live.getTableId()
                || !captured.getTablePath().equals(live.getTablePath())) {
            throw new UnionReadDataUnavailableException(
                    "Table identity changed during boundary capture.");
        }
        if (captured.getTableConfig().isDataLakeEnabled()
                && (!live.getTableConfig().isDataLakeEnabled()
                        || !captured.getLakeTablePath().equals(live.getLakeTablePath())
                        || !captured.getTableConfig()
                                .getDataLakeFormat()
                                .equals(live.getTableConfig().getDataLakeFormat()))) {
            throw new UnionReadDataUnavailableException(
                    "Lake mapping changed during boundary capture.");
        }
        validateSupported(live);
    }

    private static final class PartitionCapture {
        @Nullable private final PartitionInfo partition;
        private Map<Integer, Long> earliest = Collections.emptyMap();
        private Map<Integer, Long> stops = Collections.emptyMap();
        @Nullable private Exception error;
        @Nullable private KvSnapshots kvSnapshots;

        private PartitionCapture(@Nullable PartitionInfo partition) {
            this.partition = partition;
        }
    }
}
