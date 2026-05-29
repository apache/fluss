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

package org.apache.fluss.flink.action.orphan.build;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.flink.action.orphan.RpcErrorClassifier;
import org.apache.fluss.flink.action.orphan.rule.BucketActiveRefs;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.rpc.messages.ListKvSnapshotsResponse;
import org.apache.fluss.rpc.messages.ListRemoteLogManifestsResponse;
import org.apache.fluss.rpc.messages.PbKvSnapshot;
import org.apache.fluss.rpc.messages.PbRemoteLogManifestEntry;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.RetryUtils;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Builds the active reference set for a single {@code (tableId, partitionId|null)} target, sourced
 * from coordinator metadata via RPC (not from filesystem listing).
 *
 * <p>Log path: discovers each bucket's current remote log manifest path via {@code
 * LIST_REMOTE_LOG_MANIFESTS}, then second-reads the manifest file from object storage. The
 * per-target RPC is retried with exponential backoff via {@link RetryUtils}; per-bucket
 * second-reads make a single attempt — a {@link FileNotFoundException} (manifest upserted between
 * RPC and read) or any other IO failure immediately marks the bucket as {@link
 * LogActiveRefsFetchResult.ManifestReadStatus#READ_FAILED} and recovery is left to the next cleanup
 * round, avoiding {@code N × retries × IO} blow-up on cluster-wide turbulence.
 *
 * <p>KV path: {@code LIST_KV_SNAPSHOTS} returns snapshot ids directly (no second-read), so the
 * per-target RPC retry alone is sufficient symmetry with the log path.
 */
@Internal
public final class ActiveRefsFetcher {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String REMOTE_LOG_SEGMENTS_FIELD = "remote_log_segments";
    private static final String SEGMENT_ID_FIELD = "segment_id";
    private static final String START_OFFSET_FIELD = "start_offset";
    private static final String END_OFFSET_FIELD = "end_offset";

    /**
     * Retry backoff base used by {@link RetryUtils} for per-target RPCs. With the default 3 retries
     * and exponential backoff (200 → 400 → cap) this caps total retry delay at ~600ms — negligible
     * vs the smoothing it gives over server jitter.
     */
    private static final long DEFAULT_BACKOFF_MILLIS = 200L;

    private static final long MAX_BACKOFF_MILLIS = 2000L;

    private static final MetadataReader DEFAULT_METADATA_READER =
            new MetadataReader() {
                @Override
                public byte[] read(FsPath path) throws IOException {
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    try (FSDataInputStream inputStream = path.getFileSystem().open(path)) {
                        IOUtils.copyBytes(inputStream, outputStream);
                    }
                    return outputStream.toByteArray();
                }
            };

    private final AdminFacade admin;
    private final MetadataReader metadataReader;
    private final int maxRetries;
    private final long backoffMillis;

    public ActiveRefsFetcher(Admin admin, int maxRetries) {
        this(wrap(admin), DEFAULT_METADATA_READER, maxRetries, DEFAULT_BACKOFF_MILLIS);
    }

    public ActiveRefsFetcher(Admin admin, MetadataReader metadataReader, int maxRetries) {
        this(wrap(admin), metadataReader, maxRetries, DEFAULT_BACKOFF_MILLIS);
    }

    /** Test constructor: defaults backoff to 0 so unit tests don't pay retry sleep. */
    @VisibleForTesting
    ActiveRefsFetcher(AdminFacade admin, MetadataReader metadataReader, int maxRetries) {
        this(admin, metadataReader, maxRetries, 0L);
    }

    @VisibleForTesting
    ActiveRefsFetcher(
            AdminFacade admin, MetadataReader metadataReader, int maxRetries, long backoffMillis) {
        checkArgument(maxRetries >= 1, "maxRetries must be >= 1, got %s", maxRetries);
        checkArgument(backoffMillis >= 0L, "backoffMillis must be >= 0, got %s", backoffMillis);
        this.admin = admin;
        this.metadataReader = metadataReader;
        this.maxRetries = maxRetries;
        this.backoffMillis = backoffMillis;
    }

    private static AdminFacade wrap(Admin admin) {
        return new AdminFacade() {
            @Override
            public CompletableFuture<ListRemoteLogManifestsResponse> listRemoteLogManifests(
                    long tableId, @Nullable Long partitionId) {
                return admin.listRemoteLogManifests(tableId, partitionId);
            }

            @Override
            public CompletableFuture<ListKvSnapshotsResponse> listKvSnapshots(
                    long tableId, @Nullable Long partitionId) {
                return admin.listKvSnapshots(tableId, partitionId);
            }
        };
    }

    /**
     * Fetches per-bucket log active refs for a single {@code (tableId, partitionId|null)} target.
     * Each bucket whose remote manifest is returned by the RPC is second-read in a single attempt;
     * a {@link FileNotFoundException} or any other IO failure marks the bucket as {@link
     * LogActiveRefsFetchResult.ManifestReadStatus#READ_FAILED} without affecting siblings.
     * Per-target RPC failure (after retries) is reported via {@link
     * LogActiveRefsFetchResult#listOk()}.
     */
    public LogActiveRefsFetchResult fetchLogActiveRefsByBucket(
            long tableId, @Nullable Long partitionId) {
        ListRemoteLogManifestsResponse rpc;
        try {
            rpc =
                    RetryUtils.executeWithRetry(
                            () -> admin.listRemoteLogManifests(tableId, partitionId).get(),
                            "listRemoteLogManifests",
                            maxRetries,
                            backoffMillis,
                            MAX_BACKOFF_MILLIS,
                            e ->
                                    RpcErrorClassifier.classify(e)
                                            != RpcErrorClassifier.Category.NOT_FOUND);
        } catch (IOException e) {
            return LogActiveRefsFetchResult.listFailed(
                    formatRpcFailureReason(tableId, partitionId, e.getCause()));
        }

        Map<Integer, List<PbRemoteLogManifestEntry>> entriesByBucket = new HashMap<>();
        for (PbRemoteLogManifestEntry entry : rpc.getManifestsList()) {
            int bucketId = entry.getTableBucket().getBucketId();
            entriesByBucket.computeIfAbsent(bucketId, id -> new ArrayList<>()).add(entry);
        }

        Map<Integer, BucketActiveRefs> resolved = new HashMap<>();
        Map<Integer, String> readFailures = new HashMap<>();
        for (Map.Entry<Integer, List<PbRemoteLogManifestEntry>> bucketEntries :
                entriesByBucket.entrySet()) {
            int bucketId = bucketEntries.getKey();
            try {
                resolved.put(bucketId, buildBucketActiveRefs(bucketEntries.getValue()));
            } catch (FileNotFoundException e) {
                readFailures.put(
                        bucketId,
                        formatBucketReadFailureReason(
                                "Manifest not found (likely upserted concurrently)",
                                tableId,
                                partitionId,
                                bucketId,
                                e));
            } catch (ManifestParseException | JsonProcessingException e) {
                // Manifest payload is unreadable as JSON or violates the expected shape — corrupt
                // or schema-skewed, not a transient FS hiccup. Distinct reason so operators triage
                // separately (re-running the action will not recover).
                readFailures.put(
                        bucketId,
                        formatBucketReadFailureReason(
                                "Manifest parse failure (corrupt or unexpected schema)",
                                tableId,
                                partitionId,
                                bucketId,
                                e));
            } catch (IOException e) {
                readFailures.put(
                        bucketId,
                        formatBucketReadFailureReason(
                                "IO error reading manifest", tableId, partitionId, bucketId, e));
            }
        }
        return LogActiveRefsFetchResult.ofPerBucket(resolved, readFailures);
    }

    /**
     * Fetches the per-bucket active snapshot directories ({@code snap-{id}} names) for one {@code
     * (tableId, partitionId|null)} target. The set per bucket is the union of RETAINED and
     * STILL_IN_USE entries returned by {@link Admin#listKvSnapshots(long, Long)}. Per-target RPC
     * failure (after retries) is reported via {@link KvActiveRefsFetchResult#listOk()}, symmetric
     * with the log path.
     */
    public KvActiveRefsFetchResult fetchKvActiveSnapDirs(long tableId, @Nullable Long partitionId) {
        ListKvSnapshotsResponse rpc;
        try {
            rpc =
                    RetryUtils.executeWithRetry(
                            () -> admin.listKvSnapshots(tableId, partitionId).get(),
                            "listKvSnapshots",
                            maxRetries,
                            backoffMillis,
                            MAX_BACKOFF_MILLIS,
                            e ->
                                    RpcErrorClassifier.classify(e)
                                            != RpcErrorClassifier.Category.NOT_FOUND);
        } catch (IOException e) {
            return KvActiveRefsFetchResult.listFailed(
                    formatRpcFailureReason(tableId, partitionId, e.getCause()));
        }
        Map<Integer, Set<String>> dirsByBucket = new HashMap<>();
        for (PbKvSnapshot snapshot : rpc.getActiveSnapshotsList()) {
            int bucketId = snapshot.getBucketId();
            String dirName = FlussPaths.REMOTE_KV_SNAPSHOT_DIR_PREFIX + snapshot.getSnapshotId();
            dirsByBucket.computeIfAbsent(bucketId, b -> new HashSet<>()).add(dirName);
        }
        return KvActiveRefsFetchResult.ok(dirsByBucket);
    }

    private static String formatRpcFailureReason(
            long tableId, @Nullable Long partitionId, @Nullable Throwable cause) {
        String reason =
                String.format("RPC failure for tableId=%s partitionId=%s", tableId, partitionId);
        if (cause != null && cause.getMessage() != null) {
            reason = reason + ": " + cause.getMessage();
        }
        return reason;
    }

    private static String formatBucketReadFailureReason(
            String prefix,
            long tableId,
            @Nullable Long partitionId,
            int bucketId,
            Throwable cause) {
        String reason =
                String.format(
                        "%s for tableId=%s partitionId=%s bucketId=%s",
                        prefix, tableId, partitionId, bucketId);
        if (cause != null && cause.getMessage() != null) {
            reason = reason + ": " + cause.getMessage();
        }
        return reason;
    }

    private BucketActiveRefs buildBucketActiveRefs(List<PbRemoteLogManifestEntry> entries)
            throws IOException {
        Set<String> manifestPaths = new HashSet<>();
        Set<String> segmentRelpaths = new HashSet<>();
        for (PbRemoteLogManifestEntry entry : entries) {
            String path = entry.getRemoteLogManifestPath();
            manifestPaths.add(path);
            byte[] manifestBytes = metadataReader.read(new FsPath(path));
            segmentRelpaths.addAll(parseLogSegmentRelativePaths(manifestBytes));
        }
        return new BucketActiveRefs(segmentRelpaths, Collections.emptySet(), manifestPaths);
    }

    private Set<String> parseLogSegmentRelativePaths(byte[] manifestBytes) throws IOException {
        JsonNode root = OBJECT_MAPPER.readTree(manifestBytes);
        JsonNode segmentsNode = requiredNode(root, REMOTE_LOG_SEGMENTS_FIELD);
        Set<String> relativePaths = new HashSet<>();
        Iterator<JsonNode> iterator = segmentsNode.elements();
        while (iterator.hasNext()) {
            JsonNode segmentNode = iterator.next();
            String segmentId = requiredNode(segmentNode, SEGMENT_ID_FIELD).asText();
            long startOffset = requiredNode(segmentNode, START_OFFSET_FIELD).asLong();
            long endOffset = requiredNode(segmentNode, END_OFFSET_FIELD).asLong();
            String baseOffset = FlussPaths.filenamePrefixFromOffset(startOffset);
            String writerOffset = FlussPaths.filenamePrefixFromOffset(endOffset);

            relativePaths.add(segmentId + "/" + baseOffset + FlussPaths.LOG_FILE_SUFFIX);
            relativePaths.add(segmentId + "/" + baseOffset + FlussPaths.INDEX_FILE_SUFFIX);
            relativePaths.add(segmentId + "/" + baseOffset + FlussPaths.TIME_INDEX_FILE_SUFFIX);
            relativePaths.add(
                    segmentId + "/" + writerOffset + FlussPaths.WRITER_SNAPSHOT_FILE_SUFFIX);
        }
        return relativePaths;
    }

    private static JsonNode requiredNode(JsonNode node, String fieldName)
            throws ManifestParseException {
        JsonNode field = node.get(fieldName);
        if (field == null) {
            throw new ManifestParseException("Missing required field: " + fieldName);
        }
        return field;
    }

    /**
     * Thrown when a remote-log manifest payload is structurally invalid (missing required field,
     * wrong shape). Distinct from {@link IOException} so the bucket-read failure handler can route
     * it to the {@code "Manifest parse failure"} reason instead of the generic {@code "IO error"}
     * bucket — same skip-this-round outcome, different operator triage.
     */
    static final class ManifestParseException extends IOException {
        ManifestParseException(String message) {
            super(message);
        }
    }

    /**
     * Thin abstraction over the {@link FlussAdmin} read-only RPCs the builder depends on ({@code
     * listRemoteLogManifests} for the log active manifest, {@code listKvSnapshots} for the KV
     * active snapshot dirs). Exposed for test injection.
     */
    @VisibleForTesting
    interface AdminFacade {
        CompletableFuture<ListRemoteLogManifestsResponse> listRemoteLogManifests(
                long tableId, @Nullable Long partitionId);

        CompletableFuture<ListKvSnapshotsResponse> listKvSnapshots(
                long tableId, @Nullable Long partitionId);
    }

    /**
     * Abstraction for reading manifest files from object storage. Must throw {@link
     * FileNotFoundException} (and not a wrapped variant) when the path is absent, so the caller can
     * distinguish "manifest pointer upserted concurrently" from genuine IO failures and surface
     * each with a distinct failure reason.
     */
    @VisibleForTesting
    interface MetadataReader {
        byte[] read(FsPath path) throws IOException;
    }
}
