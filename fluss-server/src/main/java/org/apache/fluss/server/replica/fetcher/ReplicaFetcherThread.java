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

import org.apache.fluss.exception.CorruptRecordException;
import org.apache.fluss.exception.DuplicateSequenceException;
import org.apache.fluss.exception.InvalidOffsetException;
import org.apache.fluss.exception.InvalidRecordException;
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.exception.StorageException;
import org.apache.fluss.metadata.LeaderEpochOffset;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.entity.FetchLogEpochInfo;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.PbFetchLogReqForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogReqForTable;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogStorage.IndexType;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.replica.fetcher.LeaderEndpoint.FetchData;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;
import org.apache.fluss.utils.concurrent.ShutdownableThread;
import org.apache.fluss.utils.log.FairBucketStatusMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Replica fetcher thread to fetch data from leader. */
final class ReplicaFetcherThread extends ShutdownableThread {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaFetcherThread.class);

    private final ReplicaManager replicaManager;
    private final LeaderEndpoint leader;
    private final int fetchBackOffMs;

    // manually add timout logic in here, todo remove this timeout logic if
    // we support global request timeout in #279
    private final int timeoutSeconds;

    // TODO this range-robin fair map will take effect after we introduce fetch response limit size
    // in FetchLogRequest. trace id: FLUSS-56111098
    /**
     * A fair status map to store bucket fetch status. {@link TableBucket} -> {@link
     * BucketFetchStatus}.
     *
     * <p>Using this map instead of concurrent hash map to make sure each table bucket have the same
     * chance to be selected.
     */
    @GuardedBy("bucketStatusMapLock")
    private final FairBucketStatusMap<BucketFetchStatus> fairBucketStatusMap =
            new FairBucketStatusMap<>();

    private final Lock bucketStatusMapLock = new ReentrantLock();
    private final Condition bucketStatusMapCondition = bucketStatusMapLock.newCondition();

    private final TabletServerMetricGroup serverMetricGroup;

    public ReplicaFetcherThread(
            String name, ReplicaManager replicaManager, LeaderEndpoint leader, int fetchBackOffMs) {
        this(name, replicaManager, leader, fetchBackOffMs, 30);
    }

    ReplicaFetcherThread(
            String name,
            ReplicaManager replicaManager,
            LeaderEndpoint leader,
            int fetchBackOffMs,
            int timeoutSeconds) {
        super(name, false);
        this.replicaManager = replicaManager;
        this.leader = leader;
        this.fetchBackOffMs = fetchBackOffMs;
        this.timeoutSeconds = timeoutSeconds;
        this.serverMetricGroup = replicaManager.getServerMetricGroup();
    }

    public LeaderEndpoint getLeader() {
        return leader;
    }

    public int getBucketCount() {
        return fairBucketStatusMap.size();
    }

    @Override
    public void doWork() {
        maybeFetch();
        // TODO, if we support fetch from follower, we need to complete delayed fetch log operation
        // here.
    }

    private void maybeFetch() {
        Optional<FetchLogContext> fetchLogContextOpt =
                inLock(
                        bucketStatusMapLock,
                        () -> {
                            Optional<FetchLogContext> fetchLogContext = Optional.empty();
                            try {
                                fetchLogContext =
                                        leader.buildFetchLogContext(
                                                fairBucketStatusMap.bucketStatusMap());
                                if (fetchLogContext.isPresent()) {
                                    FetchLogContext context =
                                            fetchLogContext
                                                    .get()
                                                    .withFetchStates(
                                                            fairBucketStatusMap.bucketStatusMap());
                                    for (PbFetchLogReqForTable table :
                                            context.getFetchLogRequest().getTablesReqsList()) {
                                        for (PbFetchLogReqForBucket bucket :
                                                table.getBucketsReqsList()) {
                                            TableBucket tb =
                                                    new TableBucket(
                                                            table.getTableId(),
                                                            bucket.hasPartitionId()
                                                                    ? bucket.getPartitionId()
                                                                    : null,
                                                            bucket.getBucketId());
                                            try {
                                                Replica replica =
                                                        replicaManager.getReplicaOrException(tb);
                                                int epoch = replica.getLeaderEpoch();
                                                context.setLeaderEpoch(tb, epoch);
                                                if (replica.getLogTablet().isLeaderEpochEnabled()) {
                                                    bucket.setCurrentLeaderEpoch(epoch)
                                                            .setLastFetchedEpoch(
                                                                    replica.getLogTablet()
                                                                            .lastFetchedEpoch(
                                                                                    bucket
                                                                                            .getFetchOffset()));
                                                }

                                            } catch (Exception e) {
                                                LOG.error(
                                                        "Cannot read replication history for {}",
                                                        tb,
                                                        e);
                                                removeBucket(tb);
                                                return Optional.empty();
                                            }
                                        }
                                    }
                                    fetchLogContext = Optional.of(context);
                                }
                                if (!fetchLogContext.isPresent()) {
                                    LOG.trace(
                                            "There are no active buckets. Back off for {} ms before "
                                                    + "sending a fetch fetchLogRequest",
                                            fetchBackOffMs);
                                    bucketStatusMapCondition.await(
                                            fetchBackOffMs, TimeUnit.MILLISECONDS);
                                }
                            } catch (InterruptedException e) {
                                LOG.error("Interrupted while awaiting fetch back off ms.", e);
                            }
                            return fetchLogContext;
                        });

        fetchLogContextOpt.ifPresent(this::processFetchLogRequest);
    }

    void removeBuckets(Set<TableBucket> tableBuckets) throws InterruptedException {
        bucketStatusMapLock.lockInterruptibly();
        try {
            tableBuckets.forEach(fairBucketStatusMap::remove);
        } finally {
            bucketStatusMapLock.unlock();
        }
    }

    void addBuckets(Map<TableBucket, InitialFetchStatus> initialFetchStatusMap)
            throws InterruptedException {
        bucketStatusMapLock.lockInterruptibly();
        try {
            initialFetchStatusMap.forEach(
                    (tableBucket, initialFetchStatus) -> {
                        BucketFetchStatus currentStatus =
                                fairBucketStatusMap.statusValue(tableBucket);
                        BucketFetchStatus updatedStatus =
                                bucketFetchStatus(tableBucket, initialFetchStatus, currentStatus);

                        fairBucketStatusMap.updateAndMoveToEnd(tableBucket, updatedStatus);
                    });

            bucketStatusMapCondition.signalAll();
        } finally {
            bucketStatusMapLock.unlock();
        }
    }

    private void removeBucket(TableBucket tableBucket) {
        try {
            inLock(bucketStatusMapLock, () -> removeBuckets(Collections.singleton(tableBucket)));
        } catch (InterruptedException e) {
            LOG.error("Interrupted while marking bucket as failed.", e);
        }
    }

    private void delayBuckets(Set<TableBucket> buckets, long delay) throws InterruptedException {
        bucketStatusMapLock.lockInterruptibly();
        try {
            for (TableBucket tableBucket : buckets) {
                BucketFetchStatus currentFetchStatus = fairBucketStatusMap.statusValue(tableBucket);
                if (currentFetchStatus != null && !currentFetchStatus.isDelayed()) {
                    // Updating the bucket fetch status and moving to end with a new DelayedItem.
                    BucketFetchStatus updatedFetchStatus =
                            new BucketFetchStatus(
                                    currentFetchStatus.tableId(),
                                    currentFetchStatus.tablePath(),
                                    currentFetchStatus.fetchOffset(),
                                    new DelayedItem(delay));
                    fairBucketStatusMap.updateAndMoveToEnd(tableBucket, updatedFetchStatus);
                }
            }
            bucketStatusMapCondition.signalAll();
        } finally {
            bucketStatusMapLock.unlock();
        }
    }

    // TODO add fetch session to reduce the fetch request byte size.
    private void processFetchLogRequest(FetchLogContext fetchLogContext) {
        Set<TableBucket> bucketsWithError = new HashSet<>();
        FetchData responseData = null;
        FetchLogRequest fetchLogRequest = fetchLogContext.getFetchLogRequest();
        CompletableFuture<FetchData> fetchFuture = null;
        try {
            LOG.trace(
                    "Sending fetch log request {} to leader {}",
                    fetchLogRequest,
                    leader.leaderServerId());
            // TODO this need not blocking to wait fetch log complete, change to async, see
            // FLUSS-56115172.
            fetchFuture = leader.fetchLog(fetchLogContext);
            responseData = fetchFuture.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (Throwable t) {
            if (isRunning()) {
                LOG.warn(
                        "Error in response from leader server {} for fetch log request from table ids: {}",
                        leader.leaderServerId(),
                        fetchLogRequest.getTablesReqsList().stream()
                                .map(x -> x.getTableId())
                                .collect(Collectors.toSet()),
                        t);
                inLock(
                        bucketStatusMapLock,
                        () -> bucketsWithError.addAll(fairBucketStatusMap.bucketSet()));
            }
            // If the fetch timed out but the future may still complete later,
            // register a callback to release the ByteBuf when the late response arrives.
            // Without this, the pooled ByteBuf held by FetchLogResponse would never be
            // released, causing a Netty direct memory leak.
            if (fetchFuture != null) {
                fetchFuture.thenAccept(ReplicaFetcherThread::releaseFetchDataBuffer);
            }
        }

        if (responseData != null) {
            bucketStatusMapLock.lock();
            try {
                handleFetchLogResponse(
                        fetchLogContext, responseData.getFetchLogResultMap(), bucketsWithError);
            } finally {
                // release buffer handle by fetchLogResponse.
                releaseFetchDataBuffer(responseData);
                bucketStatusMapLock.unlock();
            }
        }

        if (!bucketsWithError.isEmpty()) {
            handleBucketWithError(bucketsWithError);
        }
    }

    /** Releases the ByteBuf held by a FetchLogResponse in the given FetchData. */
    private static void releaseFetchDataBuffer(FetchData data) {
        if (data != null) {
            ByteBuf parsedByteBuf = data.getFetchLogResponse().getParsedByteBuf();
            if (parsedByteBuf != null) {
                ReferenceCountUtil.safeRelease(parsedByteBuf);
            }
        }
    }

    private void handleFetchLogResponse(
            FetchLogContext context,
            Map<TableBucket, FetchLogResultForBucket> responseData,
            Set<TableBucket> replicasWithError) {
        responseData.forEach(
                (tableBucket, replicaData) -> {
                    BucketFetchStatus currentFetchStatus =
                            fairBucketStatusMap.statusValue(tableBucket);
                    if (currentFetchStatus == null
                            || !currentFetchStatus.isReadyForFetch()
                            || !context.matches(tableBucket, currentFetchStatus)) {
                        return;
                    }

                    // TODO different error using different fix way.
                    switch (replicaData.getError().error()) {
                        case NONE:
                            handleFetchLogResponseOfSuccessBucket(
                                    tableBucket,
                                    currentFetchStatus,
                                    replicaData,
                                    context.getRequest(tableBucket),
                                    context.leaderEpoch(tableBucket));
                            break;
                        case LOG_OFFSET_OUT_OF_RANGE_EXCEPTION:
                            if (!handleOutOfRangeError(
                                    tableBucket,
                                    currentFetchStatus,
                                    context.leaderEpoch(tableBucket))) {
                                replicasWithError.add(tableBucket);
                            }
                            break;
                        case NOT_LEADER_OR_FOLLOWER:
                            LOG.debug(
                                    "Remote server is not the leader for replica {}, which indicate "
                                            + "that the replica is being moved.",
                                    tableBucket);
                            replicasWithError.add(tableBucket);
                            break;
                        default:
                            LOG.error(
                                    "Error in response for fetching replica {}, error message is {}",
                                    tableBucket,
                                    replicaData.getErrorMessage());
                            replicasWithError.add(tableBucket);
                    }
                });
    }

    private void handleFetchLogResponseOfSuccessBucket(
            TableBucket tableBucket,
            BucketFetchStatus currentFetchStatus,
            FetchLogResultForBucket replicaData,
            PbFetchLogReqForBucket request,
            int expectedEpoch) {
        try {
            long nextFetchOffset = -1L;
            FetchLogEpochInfo epochInfo = replicaData.epochInfo();
            Replica currentReplica = replicaManager.getReplicaOrException(tableBucket);
            if (epochInfo != null && !currentReplica.getLogTablet().isLeaderEpochEnabled()) {
                // A response prepared before disabling must not apply an epoch-based truncation.
                return;
            }
            if (currentReplica.isLeader()
                    || currentReplica.getLeaderEpoch() != expectedEpoch
                    || (epochInfo != null && epochInfo.leaderEpoch() != expectedEpoch)) {
                return;
            }
            if (epochInfo != null && epochInfo.divergingEpoch() != null) {
                LeaderEpochOffset divergence = epochInfo.divergingEpoch();
                Optional<LeaderEpochOffset> localEnd =
                        currentReplica.getLogTablet().endOffsetForEpoch(divergence.epoch());
                if (!localEnd.isPresent()) {
                    currentReplica.invalidateFollowerEpochHistory(
                            leader.leaderServerId(), expectedEpoch);
                    return;
                }
                long truncateOffset = Math.min(divergence.offset(), localEnd.get().offset());
                if (!currentReplica.truncateFollowerToEpochOffset(
                        truncateOffset, leader.leaderServerId(), expectedEpoch)) {
                    return;
                }
                fairBucketStatusMap.updateAndMoveToEnd(
                        tableBucket,
                        new BucketFetchStatus(
                                currentFetchStatus.tableId(),
                                currentFetchStatus.tablePath(),
                                truncateOffset,
                                null));
                return;
            }
            if (replicaData.fetchFromRemote()) {
                nextFetchOffset =
                        processFetchResultFromRemoteStorage(
                                tableBucket, replicaData, expectedEpoch);
            } else {
                LogAppendInfo logAppendInfo =
                        processFetchResultFromLocalStorage(
                                tableBucket,
                                currentFetchStatus.fetchOffset(),
                                replicaData,
                                expectedEpoch);
                if (logAppendInfo.validBytes() > 0) {
                    nextFetchOffset = logAppendInfo.lastOffset() + 1;
                }
            }

            if (nextFetchOffset != -1L && fairBucketStatusMap.contains(tableBucket)) {
                BucketFetchStatus newFetchStatus =
                        new BucketFetchStatus(
                                currentFetchStatus.tableId(),
                                currentFetchStatus.tablePath(),
                                nextFetchOffset,
                                null);
                fairBucketStatusMap.updateAndMoveToEnd(tableBucket, newFetchStatus);
            }
        } catch (Exception e) {
            if (e instanceof CorruptRecordException || e instanceof InvalidRecordException) {
                // we log the error and continue to ensure if there is a corrupt record in a table
                // bucket, it does not bring the fetcher thread down and cause other table bucket to
                // also lag.
                LOG.error(
                        "Found invalid record during fetch for bucket {} at offset {}",
                        tableBucket,
                        currentFetchStatus.fetchOffset(),
                        e);
            } else if (e instanceof StorageException) {
                LOG.error(
                        "Error while processing data for bucket {} at offset {}",
                        tableBucket,
                        currentFetchStatus.fetchOffset(),
                        e);
                removeBucket(tableBucket);
            } else if (e instanceof DuplicateSequenceException
                    || e instanceof OutOfOrderSequenceException
                    || e instanceof InvalidOffsetException) {
                if (replicaData.epochInfo() != null
                        && request.hasLastFetchedEpoch()
                        && request.getLastFetchedEpoch() >= 0) {
                    LOG.error(
                            "Replication offset or sequence mismatch after epoch validation for {}",
                            tableBucket,
                            e);
                    removeBucket(tableBucket);
                    return;
                }
                // Legacy peers and unknown prefixes still use the existing recovery path.
                LOG.error(
                        "Founding recoverable error while processing data for bucket {} at offset {}, try to "
                                + "truncate to LeaderEndOffsetSnapshot",
                        tableBucket,
                        currentFetchStatus.fetchOffset(),
                        e);
                try {
                    truncateToLeaderEndOffsetSnapshot(
                            tableBucket, currentFetchStatus.tablePath(), expectedEpoch);
                } catch (Exception ex) {
                    LOG.error(
                            "Error while truncating bucket {} at offset {}",
                            tableBucket,
                            currentFetchStatus.fetchOffset(),
                            ex);
                    removeBucket(tableBucket);
                }
            } else {
                LOG.error(
                        "Unexpected error occurred while processing data for bucket {} at offset {}",
                        tableBucket,
                        currentFetchStatus.fetchOffset(),
                        e);
                removeBucket(tableBucket);
            }
        }
    }

    private void truncateToLeaderEndOffsetSnapshot(
            TableBucket tableBucket, TablePath tablePath, int expectedEpoch) throws Exception {
        long leaderLocalEndOffsetWhileBecomeLeader =
                leader.fetchLeaderEndOffsetSnapshot(tableBucket).get();
        long localLogEndOffset =
                replicaManager.getReplicaOrException(tableBucket).getLocalLogEndOffset();
        if (leaderLocalEndOffsetWhileBecomeLeader != 0L
                && leaderLocalEndOffsetWhileBecomeLeader < localLogEndOffset) {
            // truncate to leaderEndOffsetSnapshot to reset follower's WriterState and fetch offset.
            truncate(tableBucket, leaderLocalEndOffsetWhileBecomeLeader, expectedEpoch);

            // update fetch status.
            BucketFetchStatus bucketFetchStatus =
                    new BucketFetchStatus(
                            tableBucket.getTableId(),
                            tablePath,
                            leaderLocalEndOffsetWhileBecomeLeader,
                            null);
            fairBucketStatusMap.updateAndMoveToEnd(tableBucket, bucketFetchStatus);
        }
    }

    private boolean handleOutOfRangeError(
            TableBucket tableBucket, BucketFetchStatus fetchStatus, int expectedEpoch) {
        try {
            BucketFetchStatus newFetchStatus = fetchOffsetAndTruncate(tableBucket, expectedEpoch);
            fairBucketStatusMap.updateAndMoveToEnd(tableBucket, newFetchStatus);
            LOG.info(
                    "Current offset {} for table bucket {} is out of range, which typically implies "
                            + "a leader change, Reset fetch offset to {}",
                    fetchStatus.fetchOffset(),
                    tableBucket,
                    newFetchStatus.fetchOffset());
        } catch (Exception e) {
            LOG.error("Error getting fetch offset for {} due to error", tableBucket, e);
            return false;
        }
        return true;
    }

    /** Handle a replica whose offset is out of range and return a new fetch offset. */
    private BucketFetchStatus fetchOffsetAndTruncate(TableBucket tableBucket, int expectedEpoch)
            throws Exception {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        long replicaEndOffset = replica.getLocalLogEndOffset();

        /*
         * Unclean leader election: A follower goes down, in the meanwhile the leader keeps
         * appending messages. The follower comes back up, and before it has completely caught up
         * with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
         * elected as the new leader, and it starts appending messages from the client. The old
         * leader comes back up, becomes a follower, and it may discover that the current leader's
         * end offset is behind its own end offset.
         *
         * <p>In such a case, truncate the current follower's log to the current leader's end offset
         * and continue fetching.
         *
         * <p>There is a potential for a mismatch between the logs of the two replicas here. We
         * don't fix this mismatch as of now.
         */
        long leaderEndOffset =
                leader.fetchLocalLogEndOffset(tableBucket).get(timeoutSeconds, TimeUnit.SECONDS);
        if (leaderEndOffset < replicaEndOffset) {
            LOG.warn(
                    "Reset fetch offset for bucket {} from {} to current leader's latest offset {}",
                    tableBucket,
                    replicaEndOffset,
                    leaderEndOffset);
            truncate(tableBucket, leaderEndOffset, expectedEpoch);
            return new BucketFetchStatus(
                    tableBucket.getTableId(), replica.getTablePath(), leaderEndOffset, null);
        } else {
            /*
             * If the leader's log end offset is greater than the follower's log end offset,
             * there are two possibilities:
             * 1. The follower could have been down for a long time and when
             *    it starts up, its end offset could be smaller than the leader's start offset because the
             *    leader has deleted old logs (log.logEndOffset < leaderStartOffset).
             * 2. When unclean leader election occurs, it is possible that the old leader's high watermark
             *    is greater than the new leader's log end offset. So when the old leader truncates its offset
             *    to its high watermark and starts to fetch from the new leader, an OffsetOutOfRangeException
             *    will be thrown. After that some more messages are write to the new leader. While the old leader
             *    is trying to handle the OffsetOutOfRangeException and query the log end offset of the new leader,
             *    the new leader's log end offset becomes higher than the follower's log end offset.
             *
             * If the follower's current log end offset is smaller than the leader's log
             * start offset, the follower should truncate all its logs, roll out a new segment and start to fetch
             * from the current leader's log start offset since the data are all stale.
             *
             * In the second case, the follower should just keep the current log segments and retry the fetch.
             * In the second case, there will be some inconsistency of data between old and new leader. We are
             * not solving it here. If users want to have strong consistency guarantees, appropriate configurations
             * needs to be set for both tablet servers and producers.
             *
             * */
            long leaderStartOffset =
                    leader.fetchLocalLogStartOffset(tableBucket)
                            .get(timeoutSeconds, TimeUnit.SECONDS);
            LOG.warn(
                    "Reset fetch offset for bucket {} from {} to current leader's start offset {}",
                    tableBucket,
                    replicaEndOffset,
                    leaderEndOffset);
            // Only truncate log when current leader's log start offset is greater than follower's
            // log end offset.
            if (leaderStartOffset > replicaEndOffset) {
                replica.truncateFollowerFullyAndStartAt(
                        leaderStartOffset, leader.leaderServerId(), expectedEpoch);
            }

            long offsetToFetch = Math.max(leaderStartOffset, replicaEndOffset);
            return new BucketFetchStatus(
                    tableBucket.getTableId(), replica.getTablePath(), offsetToFetch, null);
        }
    }

    private void handleBucketWithError(Set<TableBucket> buckets) {
        if (!buckets.isEmpty()) {
            LOG.debug("Handling errors in processFetchLogRequest for buckets {}", buckets);
            try {
                delayBuckets(buckets, fetchBackOffMs);
            } catch (InterruptedException e) {
                LOG.error("Interrupted while handle replica with error.", e);
            }
        }
    }

    /**
     * Returns initial bucket fetch status based on current status and the provided {@link
     * InitialFetchStatus}.
     */
    private BucketFetchStatus bucketFetchStatus(
            TableBucket tableBucket,
            InitialFetchStatus initialFetchStatus,
            @Nullable BucketFetchStatus currentFetchStatus) {
        if (currentFetchStatus != null) {
            return currentFetchStatus;
        } else {
            return new BucketFetchStatus(
                    tableBucket.getTableId(),
                    initialFetchStatus.tablePath(),
                    initialFetchStatus.initOffset(),
                    null);
        }
    }

    Optional<BucketFetchStatus> fetchStatus(TableBucket tableBucket) {
        return inLock(
                bucketStatusMapLock,
                () -> Optional.ofNullable(fairBucketStatusMap.statusValue(tableBucket)));
    }

    private LogAppendInfo processFetchResultFromLocalStorage(
            TableBucket tableBucket,
            long fetchOffset,
            FetchLogResultForBucket replicaData,
            int expectedEpoch)
            throws Exception {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        LogTablet logTablet = replica.getLogTablet();

        MemoryLogRecords records = (MemoryLogRecords) replicaData.recordsOrEmpty();
        if (fetchOffset != logTablet.localLogEndOffset()) {
            throw new IllegalStateException(
                    String.format(
                            "Offset mismatch for replica %s: fetched offset %s, log end offset %s",
                            tableBucket, fetchOffset, logTablet.localLogEndOffset()));
        }

        LOG.trace(
                "Follower has replica log end offset {} for replica {}. received {} bytes of message and leader high watermark {}",
                logTablet.localLogEndOffset(),
                tableBucket,
                records.sizeInBytes(),
                replicaData.getHighWatermark());

        // Append the messages to the follower log tablet.
        LogAppendInfo logAppendInfo =
                replica.appendRecordsToFollower(
                        replicaData, leader.leaderServerId(), expectedEpoch);
        LOG.trace(
                "Follower has replica log end offset {} after appending {} bytes of messages for replica {}",
                logTablet.localLogEndOffset(),
                records.sizeInBytes(),
                tableBucket);

        serverMetricGroup.replicationBytesIn().inc(records.sizeInBytes());

        return logAppendInfo;
    }

    private long processFetchResultFromRemoteStorage(
            TableBucket tb, FetchLogResultForBucket replicaData, int expectedEpoch)
            throws Exception {
        RemoteLogFetchInfo info =
                checkNotNull(replicaData.remoteLogFetchInfo(), "RemoteLogFetchInfo is null");
        Replica replica = replicaManager.getReplicaOrException(tb);
        RemoteLogSegment segment =
                info.remoteLogSegmentList().get(info.remoteLogSegmentList().size() - 1);
        long nextOffset = segment.remoteLogEndOffset();
        File download =
                Files.createTempFile(
                                replica.getLogTablet().getLogDir().toPath(),
                                "writer-snapshot-",
                                ".tmp")
                        .toFile();
        try {
            // Download before replacing local state. A network failure must leave the fetch
            // position and the existing WAL intact.
            try (java.io.InputStream input =
                    replicaManager
                            .getRemoteLogManager()
                            .getRemoteLogStorage()
                            .fetchIndex(segment, IndexType.WRITER_ID_SNAPSHOT)) {
                Files.copy(input, download.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
            FetchLogEpochInfo epochInfo = replicaData.epochInfo();
            List<LeaderEpochOffset> epochs =
                    epochInfo == null ? Collections.emptyList() : segment.leaderEpochs();
            replica.restoreFollowerFromRemote(
                    nextOffset,
                    download,
                    epochs,
                    epochInfo == null ? nextOffset : replicaData.getHighWatermark(),
                    leader.leaderServerId(),
                    expectedEpoch);
            return nextOffset;
        } finally {
            Files.deleteIfExists(download.toPath());
        }
    }

    private void truncate(TableBucket tableBucket, long offset, int expectedEpoch) {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        LogTablet log = replica.getLogTablet();

        if (offset < log.getHighWatermark()) {
            LOG.warn(
                    "Truncating {} to offset {} below high watermark {}",
                    tableBucket,
                    offset,
                    log.getHighWatermark());
        }

        replica.truncateFollowerTo(offset, leader.leaderServerId(), expectedEpoch);
    }

    @Override
    public boolean initiateShutdown() {
        return super.initiateShutdown();
    }

    @Override
    public void awaitShutdown() throws InterruptedException {
        super.awaitShutdown();
        // We don't expect any exceptions here, but catch and log any errors to avoid failing the
        // caller, especially during shutdown. It is safe to catch the exception here without
        // causing correctness issue because we are going to shut down the thread and will not
        // re-use the leaderEndpoint anyway.
        try {
            leader.close();
        } catch (Throwable t) {
            LOG.error("Failed to close after shutting down replica fetcher thread.", t);
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        initiateShutdown();
        inLock(bucketStatusMapLock, bucketStatusMapCondition::signalAll);
        awaitShutdown();
    }
}
