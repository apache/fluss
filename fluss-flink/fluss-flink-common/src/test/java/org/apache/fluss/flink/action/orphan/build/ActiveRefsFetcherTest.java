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

import org.apache.fluss.client.metadata.ActiveKvSnapshots;
import org.apache.fluss.client.metadata.RemoteLogManifestInfo;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ActiveRefsFetcher} — log active set sourced from coordinator metadata. */
class ActiveRefsFetcherTest {

    @Test
    void emptyManifestListReturnsEmptyResult() {
        AtomicInteger rpcCalls = new AtomicInteger(0);
        StubAdmin admin = new StubAdmin(rpcCalls);
        admin.queueEmptyResponse();

        StubManifestReader reader = new StubManifestReader();

        ActiveRefsFetcher builder = new ActiveRefsFetcher(admin, reader, /* maxRetries= */ 3);
        LogActiveRefsFetchResult result = builder.fetchLogActiveRefsByBucket(7L, null);

        assertThat(result.listOk()).isTrue();
        assertThat(result.statusFor(0))
                .isEqualTo(LogActiveRefsFetchResult.ManifestReadStatus.NOT_LISTED);
        // Empty success must NOT trigger a retry — lock down call count.
        assertThat(rpcCalls.get()).isEqualTo(1);
    }

    @Test
    void fileNotFoundMarksBucketReadFailedWithoutRetry() {
        // Locks down "no per-bucket retry": a single FileNotFound on the manifest second-read
        // immediately marks the bucket READ_FAILED; recovery is left to the next cleanup round.
        // This prevents N × retries × IO blow-up during cluster-wide manifest upsert turbulence.
        FsPath p0 = new FsPath("oss://b/log/db/t-7/0/metadata/p0.manifest");
        AtomicInteger rpcCalls = new AtomicInteger(0);
        StubAdmin admin = new StubAdmin(rpcCalls);
        admin.queueResponse(p0);

        StubManifestReader reader = new StubManifestReader();
        reader.failWithNotFound(p0);

        ActiveRefsFetcher builder = new ActiveRefsFetcher(admin, reader, /* maxRetries= */ 3);
        LogActiveRefsFetchResult result = builder.fetchLogActiveRefsByBucket(7L, null);

        assertThat(result.listOk()).isTrue();
        assertThat(result.statusFor(0))
                .isEqualTo(LogActiveRefsFetchResult.ManifestReadStatus.READ_FAILED);
        assertThat(result.readFailureReason(0))
                .contains("Manifest not found (likely upserted concurrently)")
                .contains("bucketId=0");
        // Per-target RPC issued exactly once; no per-bucket retry burst.
        assertThat(rpcCalls.get()).isEqualTo(1);
    }

    @Test
    void fetchLogActiveRefsByBucket_abortsOnlyFailedBucket() throws Exception {
        FsPath p0 = new FsPath("oss://b/log/db/t-7/0/metadata/p0.manifest");
        FsPath p1 = new FsPath("oss://b/log/db/t-7/1/metadata/p1.manifest");
        String manifestJson = manifestJson("11111111-1111-1111-1111-111111111111", 7L, 9L);

        AtomicInteger rpcCalls = new AtomicInteger(0);
        StubAdmin admin = new StubAdmin(rpcCalls);
        admin.queueMultiBucketResponse(p0, p1);

        StubManifestReader reader = new StubManifestReader();
        reader.returnBytes(p0, manifestJson.getBytes(StandardCharsets.UTF_8));
        reader.failWithNotFound(p1);

        ActiveRefsFetcher builder = new ActiveRefsFetcher(admin, reader, /* maxRetries= */ 3);
        LogActiveRefsFetchResult result = builder.fetchLogActiveRefsByBucket(7L, null);

        assertThat(result.listOk()).isTrue();
        assertThat(result.statusFor(0))
                .isEqualTo(LogActiveRefsFetchResult.ManifestReadStatus.RESOLVED);
        assertThat(result.statusFor(1))
                .isEqualTo(LogActiveRefsFetchResult.ManifestReadStatus.READ_FAILED);
        assertThat(result.activeRefsOf(0).logSegmentRelativePaths())
                .containsExactlyInAnyOrder(
                        "11111111-1111-1111-1111-111111111111/"
                                + FlussPaths.filenamePrefixFromOffset(7L)
                                + ".log",
                        "11111111-1111-1111-1111-111111111111/"
                                + FlussPaths.filenamePrefixFromOffset(7L)
                                + ".index",
                        "11111111-1111-1111-1111-111111111111/"
                                + FlussPaths.filenamePrefixFromOffset(7L)
                                + ".timeindex",
                        "11111111-1111-1111-1111-111111111111/"
                                + FlussPaths.filenamePrefixFromOffset(9L)
                                + ".writer_snapshot");
        assertThat(result.readFailureReason(1))
                .contains("Manifest not found (likely upserted concurrently)")
                .contains("bucketId=1");
        assertThat(result.statusFor(2))
                .isEqualTo(LogActiveRefsFetchResult.ManifestReadStatus.NOT_LISTED);
        // Per-target RPC issued exactly once; per-bucket failure does not trigger any extra RPC.
        assertThat(rpcCalls.get()).isEqualTo(1);
    }

    @Test
    void fetchLogActiveRefsByBucket_targetRpcFailure() {
        AtomicInteger rpcCalls = new AtomicInteger(0);
        StubAdmin admin = new StubAdmin(rpcCalls);

        ActiveRefsFetcher builder =
                new ActiveRefsFetcher(admin, new StubManifestReader(), /* maxRetries= */ 3);
        LogActiveRefsFetchResult result = builder.fetchLogActiveRefsByBucket(7L, null);

        assertThat(result.listOk()).isFalse();
        assertThat(result.listFailureReason()).contains("RPC failure for tableId=7");
        // Per-bucket queries are not meaningful when listOk=false.
        assertThatThrownBy(() -> result.statusFor(0)).isInstanceOf(IllegalStateException.class);
        // Per-target RPC is retried up to maxRetries times before giving up.
        assertThat(rpcCalls.get()).isEqualTo(3);
    }

    @Test
    void manifestParseFailureMarksBucketReadFailed() {
        FsPath p0 = new FsPath("oss://b/log/db/t-7/0/metadata/p0.manifest");
        StubAdmin admin = new StubAdmin(new AtomicInteger());
        admin.queueResponse(p0);

        StubManifestReader reader = new StubManifestReader();
        reader.returnBytes(p0, "{}".getBytes(StandardCharsets.UTF_8));

        ActiveRefsFetcher builder = new ActiveRefsFetcher(admin, reader, /* maxRetries= */ 3);
        LogActiveRefsFetchResult result = builder.fetchLogActiveRefsByBucket(7L, null);

        assertThat(result.listOk()).isTrue();
        assertThat(result.statusFor(0))
                .isEqualTo(LogActiveRefsFetchResult.ManifestReadStatus.READ_FAILED);
        assertThat(result.readFailureReason(0))
                .contains("Manifest parse failure")
                .contains("bucketId=0");
    }

    @Test
    void ioErrorMarksBucketReadFailed() {
        FsPath p0 = new FsPath("oss://b/log/db/t-7/0/metadata/p0.manifest");
        StubAdmin admin = new StubAdmin(new AtomicInteger());
        admin.queueResponse(p0);

        StubManifestReader reader = new StubManifestReader();
        reader.failWithIo(p0, new IOException("disk fault"));

        ActiveRefsFetcher builder = new ActiveRefsFetcher(admin, reader, /* maxRetries= */ 3);
        LogActiveRefsFetchResult result = builder.fetchLogActiveRefsByBucket(7L, null);

        assertThat(result.listOk()).isTrue();
        assertThat(result.statusFor(0))
                .isEqualTo(LogActiveRefsFetchResult.ManifestReadStatus.READ_FAILED);
        assertThat(result.readFailureReason(0)).contains("IO error reading manifest");
    }

    @Test
    void fetchKvActiveSnapDirsAggregatesPerBucket() {
        StubAdmin admin = new StubAdmin(new AtomicInteger());
        Map<Integer, Set<Long>> snapshotIds = new HashMap<>();
        snapshotIds.put(0, new HashSet<>(Arrays.asList(9L, 10L)));
        snapshotIds.put(1, new HashSet<>(Arrays.asList(5L)));
        admin.queueKvResponseMultiBucket(snapshotIds);

        ActiveRefsFetcher builder =
                new ActiveRefsFetcher(admin, /* metadataReader */ null, /* maxRetries= */ 3);
        KvActiveRefsFetchResult result = builder.fetchKvActiveSnapDirs(7L, null);

        assertThat(result.listOk()).isTrue();
        Map<Integer, Set<String>> perBucket = result.activeSnapDirsByBucket();
        assertThat(perBucket.get(0)).containsExactlyInAnyOrder("snap-9", "snap-10");
        assertThat(perBucket.get(1)).containsExactly("snap-5");
    }

    /**
     * Symmetric with {@link #fetchLogActiveRefsByBucket_targetRpcFailure}: the KV per-target RPC
     * retries up to {@code maxRetries} times and reports {@code listOk=false} on exhaustion.
     */
    @Test
    void fetchKvActiveSnapDirsRetriesThenReportsListFailure() {
        AtomicInteger rpcCalls = new AtomicInteger(0);
        StubAdmin admin = new StubAdmin(rpcCalls);
        // No queued KV response → StubAdmin returns failed CompletableFutures on every attempt.

        ActiveRefsFetcher builder =
                new ActiveRefsFetcher(admin, /* metadataReader */ null, /* maxRetries= */ 3);
        KvActiveRefsFetchResult result = builder.fetchKvActiveSnapDirs(7L, null);

        assertThat(result.listOk()).isFalse();
        // Reason is classified via RpcErrorClassifier for audit compatibility.
        assertThat(result.listFailureReason()).isNotEmpty();
        // Per-target RPC is retried up to maxRetries times before giving up.
        assertThat(rpcCalls.get()).isEqualTo(3);
    }

    /**
     * Verifies that a non-null {@code partitionId} is forwarded to the underlying {@code
     * listRemoteLogManifests} RPC by {@link ActiveRefsFetcher#fetchLogActiveRefsByBucket}.
     */
    @Test
    void fetchLogActiveRefsByBucketWithPartitionIdRoutesCorrectly() throws Exception {
        FsPath p0 = new FsPath("oss://b/log/db/t-7/0/metadata/p0.manifest");
        String manifestJson = manifestJson("11111111-1111-1111-1111-111111111111", 7L, 9L);

        AtomicInteger rpcCalls = new AtomicInteger(0);
        StubAdmin admin = new StubAdmin(rpcCalls);
        admin.queueResponse(p0);

        StubManifestReader reader = new StubManifestReader();
        reader.returnBytes(p0, manifestJson.getBytes(StandardCharsets.UTF_8));

        ActiveRefsFetcher builder = new ActiveRefsFetcher(admin, reader, /* maxRetries= */ 3);
        LogActiveRefsFetchResult result = builder.fetchLogActiveRefsByBucket(7L, 42L);

        assertThat(result.listOk()).isTrue();
        assertThat(result.statusFor(0))
                .isEqualTo(LogActiveRefsFetchResult.ManifestReadStatus.RESOLVED);
        // Proves partitionId=42 was forwarded to the RPC (sentinel Long.MIN_VALUE would mean
        // the stub was never invoked).
        assertThat(admin.lastLogPartitionId.get())
                .as("partitionId must be forwarded to listRemoteLogManifests RPC")
                .isEqualTo(42L);
        assertThat(rpcCalls.get())
                .as("happy path must issue exactly one listRemoteLogManifests RPC")
                .isEqualTo(1);
    }

    /**
     * Verifies that a non-null {@code partitionId} is forwarded to the underlying {@code
     * listKvSnapshots} RPC by {@link ActiveRefsFetcher#fetchKvActiveSnapDirs}.
     */
    @Test
    void fetchKvActiveSnapDirsWithPartitionIdRoutesCorrectly() {
        AtomicInteger rpcCalls = new AtomicInteger(0);
        StubAdmin admin = new StubAdmin(rpcCalls);
        admin.queueKvResponse(0, 5L);

        ActiveRefsFetcher builder =
                new ActiveRefsFetcher(admin, /* metadataReader */ null, /* maxRetries= */ 3);
        KvActiveRefsFetchResult result = builder.fetchKvActiveSnapDirs(7L, 99L);

        assertThat(result.listOk()).isTrue();
        Map<Integer, Set<String>> perBucket = result.activeSnapDirsByBucket();
        assertThat(perBucket).containsOnlyKeys(0);
        assertThat(perBucket.get(0)).containsExactly("snap-5");
        // Proves partitionId=99 was forwarded to the RPC.
        assertThat(admin.lastKvPartitionId.get())
                .as("partitionId must be forwarded to listKvSnapshots RPC")
                .isEqualTo(99L);
        assertThat(rpcCalls.get())
                .as("happy path must issue exactly one listKvSnapshots RPC")
                .isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // fetchKvSharedSstFileNames tests
    // -------------------------------------------------------------------------

    @Test
    void fetchKvSharedSstFileNamesReadsMetadataAndExtractsFileNames() {
        FsPath kvTabletDir = new FsPath("oss://b/kv/db/t-7/0");
        Set<String> activeSnapDirs = new HashSet<>(Arrays.asList("snap-5", "snap-10"));

        String metadataSnap5 =
                "{\"kv_snapshot_handle\":{\"shared_file_handles\":["
                        + "{\"local_path\":\"aaa.sst\",\"size\":100},"
                        + "{\"local_path\":\"bbb.sst\",\"size\":200}"
                        + "]}}";
        String metadataSnap10 =
                "{\"kv_snapshot_handle\":{\"shared_file_handles\":["
                        + "{\"local_path\":\"bbb.sst\",\"size\":200},"
                        + "{\"local_path\":\"ccc.sst\",\"size\":300}"
                        + "]}}";

        StubManifestReader reader = new StubManifestReader();
        reader.returnBytes(
                new FsPath("oss://b/kv/db/t-7/0/snap-5/_METADATA"),
                metadataSnap5.getBytes(StandardCharsets.UTF_8));
        reader.returnBytes(
                new FsPath("oss://b/kv/db/t-7/0/snap-10/_METADATA"),
                metadataSnap10.getBytes(StandardCharsets.UTF_8));

        StubAdmin admin = new StubAdmin(new AtomicInteger());
        ActiveRefsFetcher fetcher = new ActiveRefsFetcher(admin, reader, /* maxRetries= */ 3);

        KvSharedSstFetchResult result =
                fetcher.fetchKvSharedSstFileNames(kvTabletDir, activeSnapDirs);

        assertThat(result.allMetadataReadOk()).isTrue();
        assertThat(result.sharedSstFileNames())
                .containsExactlyInAnyOrder("aaa.sst", "bbb.sst", "ccc.sst");
    }

    @Test
    void fetchKvSharedSstFileNamesReportsFailureOnFileNotFound() {
        FsPath kvTabletDir = new FsPath("oss://b/kv/db/t-7/0");
        Set<String> activeSnapDirs = new HashSet<>(Arrays.asList("snap-5", "snap-10"));

        String metadataSnap5 =
                "{\"kv_snapshot_handle\":{\"shared_file_handles\":["
                        + "{\"local_path\":\"aaa.sst\",\"size\":100}"
                        + "]}}";

        StubManifestReader reader = new StubManifestReader();
        reader.returnBytes(
                new FsPath("oss://b/kv/db/t-7/0/snap-5/_METADATA"),
                metadataSnap5.getBytes(StandardCharsets.UTF_8));
        // snap-10 metadata file not found — concurrent removal
        reader.failWithNotFound(new FsPath("oss://b/kv/db/t-7/0/snap-10/_METADATA"));

        StubAdmin admin = new StubAdmin(new AtomicInteger());
        ActiveRefsFetcher fetcher = new ActiveRefsFetcher(admin, reader, /* maxRetries= */ 3);

        KvSharedSstFetchResult result =
                fetcher.fetchKvSharedSstFileNames(kvTabletDir, activeSnapDirs);

        assertThat(result.allMetadataReadOk()).isFalse();
        assertThat(result.failureReason()).contains("Snapshot metadata not found");
        assertThat(result.sharedSstFileNames()).isEmpty();
    }

    @Test
    void fetchKvSharedSstFileNamesReportsFailureOnIoError() {
        FsPath kvTabletDir = new FsPath("oss://b/kv/db/t-7/0");
        Set<String> activeSnapDirs = new HashSet<>(Arrays.asList("snap-5"));

        StubManifestReader reader = new StubManifestReader();
        reader.failWithIo(
                new FsPath("oss://b/kv/db/t-7/0/snap-5/_METADATA"),
                new IOException("connection reset"));

        StubAdmin admin = new StubAdmin(new AtomicInteger());
        ActiveRefsFetcher fetcher = new ActiveRefsFetcher(admin, reader, /* maxRetries= */ 3);

        KvSharedSstFetchResult result =
                fetcher.fetchKvSharedSstFileNames(kvTabletDir, activeSnapDirs);

        assertThat(result.allMetadataReadOk()).isFalse();
        assertThat(result.failureReason()).contains("connection reset");
        assertThat(result.sharedSstFileNames()).isEmpty();
    }

    @Test
    void fetchKvSharedSstFileNamesReportsFailureOnParseError() {
        FsPath kvTabletDir = new FsPath("oss://b/kv/db/t-7/0");
        Set<String> activeSnapDirs = new HashSet<>(Arrays.asList("snap-5"));

        StubManifestReader reader = new StubManifestReader();
        // Invalid JSON in _METADATA
        reader.returnBytes(
                new FsPath("oss://b/kv/db/t-7/0/snap-5/_METADATA"),
                "not valid json".getBytes(StandardCharsets.UTF_8));

        StubAdmin admin = new StubAdmin(new AtomicInteger());
        ActiveRefsFetcher fetcher = new ActiveRefsFetcher(admin, reader, /* maxRetries= */ 3);

        KvSharedSstFetchResult result =
                fetcher.fetchKvSharedSstFileNames(kvTabletDir, activeSnapDirs);

        assertThat(result.allMetadataReadOk()).isFalse();
        assertThat(result.failureReason()).contains("Failed to parse snapshot metadata");
    }

    // -------------------------------------------------------------------------
    // Test fixtures
    // -------------------------------------------------------------------------

    private static String manifestJson(String segmentId, long startOffset, long endOffset) {
        return "{\"version\":1,"
                + "\"database\":\"db\","
                + "\"table\":\"t\","
                + "\"table_id\":7,"
                + "\"bucket_id\":0,"
                + "\"remote_log_segments\":[{"
                + "\"segment_id\":\""
                + segmentId
                + "\",\"start_offset\":"
                + startOffset
                + ",\"end_offset\":"
                + endOffset
                + ",\"max_timestamp\":0,"
                + "\"size_in_bytes\":1"
                + "}]}";
    }

    /** Queues per-call responses for ListRemoteLogManifests / ListKvSnapshots and tracks calls. */
    private static final class StubAdmin implements ActiveRefsFetcher.AdminFacade {

        private final Deque<List<RemoteLogManifestInfo>> responses = new ArrayDeque<>();
        private final Deque<ActiveKvSnapshots> kvResponses = new ArrayDeque<>();
        private final AtomicInteger callCounter;
        private final AtomicReference<Long> lastLogPartitionId =
                new AtomicReference<>(Long.MIN_VALUE);
        private final AtomicReference<Long> lastKvPartitionId =
                new AtomicReference<>(Long.MIN_VALUE);

        StubAdmin(AtomicInteger callCounter) {
            this.callCounter = callCounter;
        }

        void queueResponse(FsPath manifestPath) {
            queueResponse(manifestPath, 0);
        }

        void queueResponse(FsPath manifestPath, int bucketId) {
            List<RemoteLogManifestInfo> list = new ArrayList<>();
            list.add(
                    new RemoteLogManifestInfo(
                            new TableBucket(7L, bucketId), manifestPath.toString(), 0L));
            responses.add(list);
        }

        void queueMultiBucketResponse(FsPath manifestPath0, FsPath manifestPath1) {
            List<RemoteLogManifestInfo> list = new ArrayList<>();
            list.add(
                    new RemoteLogManifestInfo(
                            new TableBucket(7L, 0), manifestPath0.toString(), 0L));
            list.add(
                    new RemoteLogManifestInfo(
                            new TableBucket(7L, 1), manifestPath1.toString(), 0L));
            responses.add(list);
        }

        void queueEmptyResponse() {
            responses.add(Collections.emptyList());
        }

        void queueKvResponse(int bucketId, long... snapshotIds) {
            Map<Integer, Set<Long>> snapshotIdsByBucket = new HashMap<>();
            Set<Long> ids = new HashSet<>();
            for (long id : snapshotIds) {
                ids.add(id);
            }
            snapshotIdsByBucket.put(bucketId, ids);
            kvResponses.add(new ActiveKvSnapshots(7L, null, snapshotIdsByBucket));
        }

        void queueKvResponseMultiBucket(Map<Integer, Set<Long>> snapshotIdsByBucket) {
            kvResponses.add(new ActiveKvSnapshots(7L, null, snapshotIdsByBucket));
        }

        @Override
        public CompletableFuture<List<RemoteLogManifestInfo>> listRemoteLogManifests(
                long tableId, @Nullable Long partitionId) {
            callCounter.incrementAndGet();
            lastLogPartitionId.set(partitionId);
            List<RemoteLogManifestInfo> next = responses.poll();
            if (next == null) {
                CompletableFuture<List<RemoteLogManifestInfo>> failed = new CompletableFuture<>();
                failed.completeExceptionally(
                        new IllegalStateException("StubAdmin: no more queued responses"));
                return failed;
            }
            return CompletableFuture.completedFuture(next);
        }

        @Override
        public CompletableFuture<ActiveKvSnapshots> listKvSnapshots(
                long tableId, @Nullable Long partitionId) {
            callCounter.incrementAndGet();
            lastKvPartitionId.set(partitionId);
            ActiveKvSnapshots next = kvResponses.poll();
            if (next == null) {
                CompletableFuture<ActiveKvSnapshots> failed = new CompletableFuture<>();
                failed.completeExceptionally(
                        new IllegalStateException("StubAdmin: no more queued kv responses"));
                return failed;
            }
            return CompletableFuture.completedFuture(next);
        }
    }

    /** Per-path file-content / failure registry for the second-read step. */
    private static final class StubManifestReader implements ActiveRefsFetcher.MetadataReader {

        private final Map<String, byte[]> bytesByPath = new HashMap<>();
        private final Set<String> notFoundPaths = new HashSet<>();
        private final Map<String, IOException> ioFailuresByPath = new HashMap<>();

        void returnBytes(FsPath path, byte[] data) {
            bytesByPath.put(path.toString(), data);
        }

        void failWithNotFound(FsPath path) {
            notFoundPaths.add(path.toString());
        }

        void failWithIo(FsPath path, IOException e) {
            ioFailuresByPath.put(path.toString(), e);
        }

        @Override
        public byte[] read(FsPath path) throws IOException {
            String key = path.toString();
            if (notFoundPaths.contains(key)) {
                throw new FileNotFoundException(key);
            }
            IOException io = ioFailuresByPath.get(key);
            if (io != null) {
                throw io;
            }
            byte[] data = bytesByPath.get(key);
            if (data == null) {
                throw new FileNotFoundException(key);
            }
            return data;
        }
    }
}
