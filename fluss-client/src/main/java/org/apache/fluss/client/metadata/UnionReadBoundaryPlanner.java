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
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Captures bounded UnionRead inputs and encodes them for transport to an execution engine.
 *
 * <p>Calls use the supplied Admin without closing it or opening lake files. The result fixes a
 * schema, an optional lake snapshot and per-bucket log ranges; it is neither a cross-bucket
 * transactional snapshot nor a retention lease. Without a lake baseline, PK reads reconstruct only
 * keys represented in retained logs. KV snapshot fallback and shared historical logs are not yet
 * supported. Consumers must enforce the captured partition scope on both lake and log inputs.
 *
 * <p>Concurrent calls have independent capture state. Synchronous offset request setup runs on the
 * CompletableFuture default async executor, outside the Admin RPC callback thread. Cancelling the
 * returned future does not guarantee cancellation of Admin RPCs already issued or composed by this
 * capture.
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
     * @return a future containing credential-free UTF-8 context v1 JSON; metadata and lookup
     *     failures complete it exceptionally
     */
    public static CompletableFuture<byte[]> captureReadContext(Admin admin, TablePath tablePath) {
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
     * @return a future containing context v1 JSON; invalid scope, changed table identity,
     *     unavailable metadata or inconsistent offsets complete it exceptionally
     * @throws NullPointerException if a required argument is null
     */
    public static CompletableFuture<byte[]> captureReadContext(
            Admin admin, TablePath tablePath, PartitionSpec scope) {
        checkNotNull(admin, "admin");
        checkNotNull(tablePath, "tablePath");
        checkNotNull(scope, "scope");
        PartitionSpec capturedScope = new PartitionSpec(new HashMap<>(scope.getSpecMap()));
        return admin.getTableInfo(tablePath)
                .thenCompose(
                        table -> {
                            checkArgument(
                                    table.getPartitionKeys()
                                                    .containsAll(
                                                            capturedScope.getSpecMap().keySet())
                                            && !capturedScope.getSpecMap().containsValue(null),
                                    "Scope must reference partition keys with non-null encoded values.");
                            return readableSnapshot(admin, table)
                                    .thenCompose(
                                            snapshot ->
                                                    capture(admin, table, snapshot, capturedScope));
                        });
    }

    private static CompletableFuture<LakeSnapshot> readableSnapshot(Admin admin, TableInfo table) {
        if (!table.getTableConfig().isDataLakeEnabled()) {
            return CompletableFuture.completedFuture(null);
        }
        return admin.getReadableLakeSnapshot(table.getTablePath())
                .handle(
                        (snapshot, error) -> {
                            if (error == null) {
                                return snapshot;
                            }
                            Throwable cause = ExceptionUtils.stripCompletionException(error);
                            if (cause instanceof LakeTableSnapshotNotExistException) {
                                return null;
                            }
                            throw new CompletionException(cause);
                        });
    }

    private static CompletableFuture<byte[]> capture(
            Admin admin, TableInfo table, @Nullable LakeSnapshot snapshot, PartitionSpec scope) {
        CompletableFuture<List<PartitionInfo>> partitions =
                table.isPartitioned()
                        ? admin.listPartitionInfos(table.getTablePath(), scope)
                        : CompletableFuture.completedFuture(Collections.singletonList(null));
        return partitions.thenCompose(
                selected -> {
                    CompletableFuture<List<PartitionCapture>> pending =
                            CompletableFuture.completedFuture(new ArrayList<>());
                    // Keep one partition in flight per capture.
                    for (PartitionInfo partition : selected) {
                        pending =
                                pending.thenCompose(
                                        captures ->
                                                readOffsets(admin, table, partition)
                                                        .thenApply(
                                                                capture -> {
                                                                    captures.add(capture);
                                                                    return captures;
                                                                }));
                    }
                    return pending.thenCompose(
                                    captures -> recheckPartitions(admin, table, scope, captures))
                            .thenCompose(
                                    retained ->
                                            finishCapture(admin, table, snapshot, scope, retained));
                });
    }

    private static CompletableFuture<byte[]> finishCapture(
            Admin admin,
            TableInfo table,
            @Nullable LakeSnapshot snapshot,
            PartitionSpec scope,
            List<PartitionCapture> retained) {
        return admin.getTableInfo(table.getTablePath())
                .thenApply(
                        live -> {
                            validateIdentity(table, live);
                            List<PartitionInfo> partitions = new ArrayList<>();
                            Map<TableBucket, Long> earliest = new HashMap<>();
                            Map<TableBucket, Long> stops = new HashMap<>();
                            for (PartitionCapture capture : retained) {
                                if (capture.partition != null) {
                                    partitions.add(capture.partition);
                                }
                                earliest.putAll(capture.earliest);
                                stops.putAll(capture.stops);
                            }
                            return UnionReadContextEncoder.encode(
                                    table, snapshot, partitions, earliest, stops, scope);
                        });
    }

    private static CompletableFuture<PartitionCapture> readOffsets(
            Admin admin, TableInfo table, @Nullable PartitionInfo partition) {
        PartitionCapture capture = new PartitionCapture(partition);
        // Capture synchronous request failures too, so the ID recheck can distinguish deletion.
        return CompletableFuture.completedFuture(null)
                .thenCompose(
                        ignored -> {
                            int count =
                                    partition == null
                                            ? table.getNumBuckets()
                                            : partition.getBucketCount();
                            checkArgument(count > 0, "Invalid bucket count: %s", count);
                            List<Integer> buckets =
                                    IntStream.range(0, count).boxed().collect(Collectors.toList());
                            return offsets(
                                            admin,
                                            table.getTablePath(),
                                            partition,
                                            buckets,
                                            new OffsetSpec.EarliestSpec())
                                    .thenCompose(
                                            earliest -> {
                                                capture.putOffsets(
                                                        table.getTableId(),
                                                        earliest,
                                                        capture.earliest);
                                                return offsets(
                                                        admin,
                                                        table.getTablePath(),
                                                        partition,
                                                        buckets,
                                                        new OffsetSpec.LatestSpec());
                                            });
                        })
                .handle(
                        (stops, error) -> {
                            if (error != null) {
                                capture.error = ExceptionUtils.stripCompletionException(error);
                            } else {
                                capture.putOffsets(table.getTableId(), stops, capture.stops);
                            }
                            return capture;
                        });
    }

    private static CompletableFuture<Map<Integer, Long>> offsets(
            Admin admin,
            TablePath path,
            @Nullable PartitionInfo partition,
            List<Integer> buckets,
            OffsetSpec spec) {
        // Admin.listOffsets resolves routing with blocking metadata calls. Never invoke it
        // on the Netty thread completing the previous RPC in this pipeline.
        return CompletableFuture.supplyAsync(
                        () ->
                                partition == null
                                        ? admin.listOffsets(path, buckets, spec)
                                        : admin.listOffsets(
                                                path, partition.getPartitionName(), buckets, spec))
                .thenCompose(ListOffsetsResult::all);
    }

    private static CompletableFuture<List<PartitionCapture>> recheckPartitions(
            Admin admin, TableInfo table, PartitionSpec scope, List<PartitionCapture> captures) {
        CompletableFuture<List<PartitionInfo>> recheck =
                CompletableFuture.completedFuture(null)
                        .thenCompose(
                                ignored ->
                                        table.isPartitioned()
                                                ? admin.listPartitionInfos(
                                                        table.getTablePath(), scope)
                                                : CompletableFuture.completedFuture(
                                                        Collections.emptyList()));
        return recheck.handle(
                (live, recheckError) -> {
                    Set<Long> ids = new HashSet<>();
                    if (recheckError == null) {
                        live.forEach(partition -> ids.add(partition.getPartitionId()));
                    }
                    List<PartitionCapture> retained = new ArrayList<>();
                    for (PartitionCapture capture : captures) {
                        if (recheckError == null
                                && capture.partition != null
                                && !ids.contains(capture.partition.getPartitionId())) {
                            continue;
                        }
                        if (capture.error != null) {
                            throw new CompletionException(capture.error);
                        }
                        retained.add(capture);
                    }
                    if (recheckError != null) {
                        throw new CompletionException(
                                ExceptionUtils.stripCompletionException(recheckError));
                    }
                    return retained;
                });
    }

    private static void validateIdentity(TableInfo captured, TableInfo live) {
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
        @Nullable private Throwable error;

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
