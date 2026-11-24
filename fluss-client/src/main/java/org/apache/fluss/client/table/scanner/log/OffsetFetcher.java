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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.exception.InvalidBucketRoutingException;
import org.apache.fluss.exception.InvalidMetadataException;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListOffsetsResponse;
import org.apache.fluss.rpc.messages.PbListOffsetsRespForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makeListOffsetsRequest;
import static org.apache.fluss.utils.ExceptionUtils.stripFutureException;

/**
 * Resolves a log scanner's latest-offset subscriptions and resets using asynchronous ListOffsets
 * requests.
 */
final class OffsetFetcher implements AutoCloseable {

    private static final long RETRY_BACKOFF_MS = 100L;

    private final LogScannerStatus logScannerStatus;
    private final MetadataUpdater metadataUpdater;
    private final Consumer<Collection<TableBucket>> refreshMetadata;
    private final LogFetchBuffer logFetchBuffer;

    // Callbacks that have already checked this flag may finish after close.
    private volatile boolean closed;

    OffsetFetcher(
            LogScannerStatus logScannerStatus,
            MetadataUpdater metadataUpdater,
            Consumer<Collection<TableBucket>> refreshMetadata,
            LogFetchBuffer logFetchBuffer) {
        this.logScannerStatus = logScannerStatus;
        this.metadataUpdater = metadataUpdater;
        this.refreshMetadata = refreshMetadata;
        this.logFetchBuffer = logFetchBuffer;
    }

    /** Called by poll while it holds the scanner's guard against concurrent close. */
    void resetPositionsIfNeeded() {
        Map<TableBucket, BucketScanStatus> resets =
                logScannerStatus.beginOffsetResets(System.currentTimeMillis());
        if (resets.isEmpty()) {
            return;
        }
        Map<TableOrPartition, Map<TableBucket, BucketScanStatus>> groups = new HashMap<>();
        resets.forEach(
                (bucket, assignment) ->
                        groups.computeIfAbsent(
                                        TableOrPartition.of(
                                                bucket.getTableId(), bucket.getPartitionId()),
                                        ignored -> new HashMap<>())
                                .put(bucket, assignment));
        // Send requests to all known leaders before waiting for any missing route.
        List<Map<TableBucket, BucketScanStatus>> missingGroups = new ArrayList<>();
        for (Map<TableBucket, BucketScanStatus> group : groups.values()) {
            Map<TableBucket, BucketScanStatus> missing = sendToKnownLeaders(group);
            if (!missing.isEmpty()) {
                missingGroups.add(missing);
            }
        }
        missingGroups.forEach(this::refreshAndSendLatestOffsets);
    }

    /** Sends one request per leader and returns the buckets whose routes are still missing. */
    private Map<TableBucket, BucketScanStatus> sendToKnownLeaders(
            Map<TableBucket, BucketScanStatus> group) {
        Cluster cluster = metadataUpdater.getCluster();
        Map<Integer, Map<TableBucket, BucketScanStatus>> byLeader = new HashMap<>();
        Map<TableBucket, BucketScanStatus> missing = new HashMap<>();
        group.forEach(
                (bucket, assignment) -> {
                    Integer leader = cluster.leaderFor(bucket);
                    if (leader == null) {
                        missing.put(bucket, assignment);
                    } else {
                        byLeader.computeIfAbsent(leader, ignored -> new HashMap<>())
                                .put(bucket, assignment);
                    }
                });
        byLeader.forEach((leader, buckets) -> sendLatestOffsetsToLeader(leader, cluster, buckets));
        return missing;
    }

    private void refreshAndSendLatestOffsets(Map<TableBucket, BucketScanStatus> missing) {
        try {
            refreshMetadata.accept(missing.keySet());
            sendToKnownLeaders(missing)
                    .forEach(
                            (bucket, assignment) ->
                                    failOffsetReset(
                                            bucket,
                                            assignment,
                                            new LeaderNotAvailableException(
                                                    "No leader for bucket " + bucket)));
        } catch (Exception e) {
            missing.forEach((bucket, assignment) -> failOffsetReset(bucket, assignment, e));
        }
    }

    private void sendLatestOffsetsToLeader(
            int leader, Cluster cluster, Map<TableBucket, BucketScanStatus> assignments) {
        if (closed) {
            return;
        }
        TableBucket first = assignments.keySet().iterator().next();
        List<Integer> bucketIds = new ArrayList<>(assignments.size());
        assignments.keySet().forEach(bucket -> bucketIds.add(bucket.getBucket()));
        try {
            TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
            if (gateway == null) {
                throw new LeaderNotAvailableException("No tablet server for leader " + leader);
            }
            ListOffsetsRequest request =
                    makeListOffsetsRequest(
                            first.getTableId(),
                            first.getPartitionId(),
                            bucketIds,
                            new OffsetSpec.LatestSpec(),
                            cluster);
            gateway.listOffsets(request)
                    .whenComplete(
                            (response, error) -> {
                                if (error != null) {
                                    assignments.forEach(
                                            (bucket, assignment) ->
                                                    failOffsetReset(bucket, assignment, error));
                                } else {
                                    handleLatestOffsets(first, assignments, response);
                                }
                            });
        } catch (Exception e) {
            assignments.forEach((bucket, assignment) -> failOffsetReset(bucket, assignment, e));
        }
    }

    private void handleLatestOffsets(
            TableBucket first,
            Map<TableBucket, BucketScanStatus> assignments,
            ListOffsetsResponse response) {
        if (closed) {
            return;
        }
        for (PbListOffsetsRespForBucket result : response.getBucketsRespsList()) {
            TableBucket bucket =
                    new TableBucket(
                            first.getTableId(), first.getPartitionId(), result.getBucketId());
            BucketScanStatus assignment = assignments.remove(bucket);
            if (assignment == null) {
                continue;
            }
            if (result.hasErrorCode() && result.getErrorCode() != Errors.NONE.code()) {
                failOffsetReset(bucket, assignment, ApiError.fromErrorMessage(result).exception());
            } else if (result.hasOffset() && result.getOffset() >= 0) {
                logScannerStatus.completeOffsetReset(bucket, assignment, result.getOffset());
            } else {
                failOffsetReset(
                        bucket,
                        assignment,
                        new IllegalStateException("Missing latest offset for " + bucket));
            }
        }
        assignments.forEach(
                (bucket, assignment) ->
                        failOffsetReset(
                                bucket,
                                assignment,
                                new IllegalStateException(
                                        "ListOffsets response omitted " + bucket)));
        logFetchBuffer.signalOffsetReset();
    }

    private void failOffsetReset(TableBucket bucket, BucketScanStatus assignment, Throwable error) {
        if (closed) {
            return;
        }
        Throwable cause = stripFutureException(error);
        boolean staleRoute =
                cause instanceof InvalidMetadataException
                        || cause instanceof InvalidBucketRoutingException;
        boolean retry = staleRoute || cause instanceof RetriableException;
        long retryAtMs = retry ? System.currentTimeMillis() + RETRY_BACKOFF_MS : 0L;
        if (logScannerStatus.failOffsetReset(bucket, assignment, retry ? null : cause, retryAtMs)) {
            if (staleRoute) {
                metadataUpdater
                        .getBucketLocation(bucket)
                        .map(BucketLocation::getPhysicalTablePath)
                        .ifPresent(
                                path ->
                                        metadataUpdater.invalidPhysicalTableBucketMeta(
                                                Collections.singleton(path)));
            }
            logFetchBuffer.signalOffsetReset();
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}
