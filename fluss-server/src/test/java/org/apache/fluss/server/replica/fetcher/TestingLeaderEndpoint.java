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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFetchLogData;
import static org.apache.fluss.utils.function.ThrowingRunnable.unchecked;

/** The leader end point used for test, which replica manager in local. */
public class TestingLeaderEndpoint implements LeaderEndpoint {

    private final ReplicaManager replicaManager;
    private final ServerNode localNode;
    /** The max size for the fetch response. */
    private final int maxFetchSize;
    /** The max fetch size for a bucket in bytes. */
    private final int maxFetchSizeForBucket;

    /**
     * If set, fetchLog will return a future that completes after the specified delay, simulating a
     * slow leader. The response will carry a pooled ByteBuf to track buffer release.
     */
    private volatile ScheduledExecutorService delayExecutor;

    private volatile long delayMs;

    /** All ByteBufs allocated for delayed fetch responses, for leak detection in tests. */
    private final java.util.concurrent.CopyOnWriteArrayList<ByteBuf> allocatedByteBufs =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    public TestingLeaderEndpoint(
            Configuration conf, ReplicaManager replicaManager, ServerNode localNode) {
        this.replicaManager = replicaManager;
        this.localNode = localNode;
        this.maxFetchSize = (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MAX_BYTES).getBytes();
        this.maxFetchSizeForBucket =
                (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MAX_BYTES_FOR_BUCKET).getBytes();
    }

    @Override
    public int leaderServerId() {
        return localNode.id();
    }

    @Override
    public CompletableFuture<Long> fetchLocalLogEndOffset(TableBucket tableBucket) {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        return CompletableFuture.completedFuture(replica.getLocalLogEndOffset());
    }

    @Override
    public CompletableFuture<Long> fetchLocalLogStartOffset(TableBucket tableBucket) {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        return CompletableFuture.completedFuture(replica.getLocalLogStartOffset());
    }

    @Override
    public CompletableFuture<Long> fetchLeaderEndOffsetSnapshot(TableBucket tableBucket) {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        return CompletableFuture.completedFuture(replica.getLeaderEndOffsetSnapshot());
    }

    @Override
    public CompletableFuture<FetchData> fetchLog(FetchLogContext fetchLogContext) {
        CompletableFuture<FetchData> response = new CompletableFuture<>();
        FetchLogRequest fetchLogRequest = fetchLogContext.getFetchLogRequest();
        Map<TableBucket, FetchReqInfo> fetchLogData = getFetchLogData(fetchLogRequest);
        replicaManager.fetchLogRecords(
                new FetchParams(
                        fetchLogRequest.getFollowerServerId(), fetchLogRequest.getMaxBytes()),
                fetchLogData,
                null,
                result -> {
                    FetchData fetchData = createFetchData(processResult(result));
                    if (delayExecutor != null && delayMs > 0) {
                        // Simulate a slow leader: complete the future after a delay
                        delayExecutor.schedule(
                                () -> response.complete(fetchData), delayMs, TimeUnit.MILLISECONDS);
                    } else {
                        response.complete(fetchData);
                    }
                });
        return response;
    }

    /**
     * Enables delayed fetch responses to simulate a slow/unreachable leader.
     *
     * @param executor the scheduler to use for delayed completion
     * @param delayMs the delay in milliseconds before completing the fetch future
     */
    public void setFetchDelay(ScheduledExecutorService executor, long delayMs) {
        this.delayExecutor = executor;
        this.delayMs = delayMs;
    }

    /** Clears the fetch delay, returning to immediate completion. */
    public void clearFetchDelay() {
        this.delayExecutor = null;
        this.delayMs = 0;
    }

    /** Returns all pooled ByteBufs allocated for delayed fetch responses. */
    public java.util.List<ByteBuf> getAllAllocatedByteBufs() {
        return allocatedByteBufs;
    }

    /**
     * Creates a FetchData with a FetchLogResponse that holds a pooled ByteBuf, simulating what
     * NettyClientHandler does for inner client FetchLogResponse (zero-copy path).
     */
    private FetchData createFetchData(Map<TableBucket, FetchLogResultForBucket> resultMap) {
        FetchLogResponse fetchLogResponse = new FetchLogResponse();
        if (delayExecutor != null && delayMs > 0) {
            // Simulate the real NettyClientHandler behavior: the FetchLogResponse holds a
            // pooled ByteBuf that must be manually released via getParsedByteBuf().release().
            ByteBuf pooledBuf = PooledByteBufAllocator.DEFAULT.buffer(64);
            pooledBuf.writeZero(64);
            // Parse from the pooled buffer so that getParsedByteBuf() returns it
            fetchLogResponse.parseFrom(pooledBuf, 0);
            allocatedByteBufs.add(pooledBuf);
        }
        return new FetchData(fetchLogResponse, resultMap);
    }

    @Override
    public Optional<FetchLogContext> buildFetchLogContext(
            Map<TableBucket, BucketFetchStatus> replicas) {
        return RemoteLeaderEndpoint.buildFetchLogContext(
                replicas, localNode.id(), maxFetchSize, maxFetchSizeForBucket, -1, -1);
    }

    @Override
    public void close() {
        // nothing to do now.
    }

    /** Convert FileLogRecords to MemoryLogRecords. */
    private Map<TableBucket, FetchLogResultForBucket> processResult(
            Map<TableBucket, FetchLogResultForBucket> fetchDataMap) {
        Map<TableBucket, FetchLogResultForBucket> result = new HashMap<>();
        fetchDataMap.forEach(
                (tb, value) -> {
                    LogRecords logRecords = value.recordsOrEmpty();
                    if (logRecords instanceof FileLogRecords) {
                        FileLogRecords fileRecords = (FileLogRecords) logRecords;
                        // convert FileLogRecords to MemoryLogRecords
                        ByteBuffer buffer = ByteBuffer.allocate(fileRecords.sizeInBytes());
                        unchecked(() -> fileRecords.readInto(buffer, 0)).run();
                        MemoryLogRecords memRecords = MemoryLogRecords.pointToByteBuffer(buffer);
                        result.put(
                                tb,
                                new FetchLogResultForBucket(
                                        tb, memRecords, value.getHighWatermark()));
                    } else {
                        result.put(tb, value);
                    }
                });

        return result;
    }
}
