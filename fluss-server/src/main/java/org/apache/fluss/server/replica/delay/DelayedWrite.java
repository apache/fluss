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

package org.apache.fluss.server.replica.delay;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.WriteResultForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A delayed write operation that can be created by the {@link ReplicaManager} and watched in the
 * delayed write operation manager. delayed write operation can be sent by {@link ProduceLogRequest}
 * or {@link PutKvRequest}.
 */
public class DelayedWrite<T extends WriteResultForBucket> extends DelayedOperation {
    private static final Logger LOG = LoggerFactory.getLogger(DelayedWrite.class);

    private final DelayedWriteMetadata<T> delayedWriteMetadata;
    private final ReplicaManager replicaManager;
    private final Consumer<List<T>> callback;
    private final TabletServerMetricGroup serverMetricGroup;

    public DelayedWrite(
            long delayMs,
            DelayedWriteMetadata<T> delayedWriteMetadata,
            ReplicaManager replicaManager,
            Consumer<List<T>> callback,
            TabletServerMetricGroup serverMetricGroup) {
        super(delayMs);
        this.delayedWriteMetadata = delayedWriteMetadata;
        this.replicaManager = replicaManager;
        this.callback = callback;
        this.serverMetricGroup = serverMetricGroup;

        // first update the acks pending variable according to the error code.
        updateStatus();
    }

    private void updateStatus() {
        delayedWriteMetadata
                .getBucketStatusMap()
                .forEach(
                        ((tableBucket, delayedBucketStatus) -> {
                            WriteResultForBucket writeResult =
                                    delayedBucketStatus.getWriteResultForBucket();
                            if (writeResult.succeeded()) {
                                // Timeout error state will be cleared when required acks are
                                // received.
                                delayedBucketStatus.setAcksPending(true);
                                delayedBucketStatus.setDelayedError(Errors.REQUEST_TIME_OUT);
                            } else {
                                delayedBucketStatus.setAcksPending(false);
                            }

                            LOG.trace(
                                    "Initial bucket status for {} is {} before register as delay write operation.",
                                    tableBucket,
                                    delayedBucketStatus);
                        }));
    }

    /**
     * The delayed write operation can be completed if every bucket it writes to is satisfied by one
     * of the following:
     *
     * <pre>
     *     Case A: Replica not assigned to this tablet server.
     *     Case B: Replica is no longer the leader of this bucket.
     *     Case C: This tablet server is the leader.
     *       C.1 - If there was a local error thrown while checking if at least requiredAcks
     *       replicas have caught up to this operation: set an error in response.
     *       C.2 - Otherwise, set the response with no error.
     * </pre>
     */
    @Override
    public boolean tryComplete() {
        // check for each bucket if it still has pending acks.
        boolean allBucketsSatisfied = true;
        for (Map.Entry<TableBucket, DelayedBucketStatus<T>> entry :
                delayedWriteMetadata.getBucketStatusMap().entrySet()) {
            TableBucket tableBucket = entry.getKey();
            DelayedBucketStatus<T> delayedBucketStatus = entry.getValue();
            LOG.trace(
                    "Checking write operation satisfaction for {}, current status {}",
                    tableBucket,
                    delayedBucketStatus);

            // skip those buckets that have already been satisfied.
            if (delayedBucketStatus.isAcksPending()) {
                Tuple2<Boolean, Errors> result;
                try {
                    Replica replica = replicaManager.getReplicaOrException(tableBucket);
                    result =
                            replica.checkEnoughReplicasReachOffset(
                                    delayedBucketStatus.getRequiredOffset());
                } catch (Exception e) {
                    result = Tuple2.of(false, Errors.forException(e));
                }

                // Case B || C.1 || C.2.
                Errors errors = result.f1;
                if (errors != Errors.NONE || result.f0) {
                    delayedBucketStatus.setAcksPending(false);
                    delayedBucketStatus.setDelayedError(errors);
                }

                if (delayedBucketStatus.isAcksPending()) {
                    allBucketsSatisfied = false;
                }
            }
        }

        if (allBucketsSatisfied) {
            return forceComplete();
        } else {
            return false;
        }
    }

    @Override
    public void onExpiration() {
        serverMetricGroup.delayedWriteExpireCount().inc();
        delayedWriteMetadata
                .getBucketStatusMap()
                .forEach(
                        (tableBucket, delayedBucketStatus) ->
                                LOG.debug(
                                        "Expiring delay write operation for bucket {} with status {}",
                                        tableBucket,
                                        delayedBucketStatus));
    }

    /** Upon completion, return the current response status along with the error code per bucket. */
    @Override
    public void onComplete() {
        List<T> result =
                delayedWriteMetadata.getBucketStatusMap().values().stream()
                        // overwrite the write result with the delayed error if there is one.
                        .map(
                                s -> {
                                    Errors error = s.getDelayedError();
                                    if (error != null && error != Errors.NONE) {
                                        return s.getWriteResultForBucket().copy(error);
                                    } else {
                                        return s.getWriteResultForBucket();
                                    }
                                })
                        .collect(Collectors.toList());
        callback.accept(result);
    }

    /** DelayedWriteMetadata. */
    public static final class DelayedWriteMetadata<T extends WriteResultForBucket> {
        private final int requiredAcks;
        private final Map<TableBucket, DelayedBucketStatus<T>> bucketStatusMap;

        public DelayedWriteMetadata(
                int requiredAcks, Map<TableBucket, DelayedBucketStatus<T>> bucketStatusMap) {
            this.requiredAcks = requiredAcks;
            this.bucketStatusMap = bucketStatusMap;
        }

        public Map<TableBucket, DelayedBucketStatus<T>> getBucketStatusMap() {
            return bucketStatusMap;
        }

        @Override
        public String toString() {
            return "DelayedWriteMetadata{"
                    + "requiredAcks="
                    + requiredAcks
                    + ", bucketStatusMap="
                    + bucketStatusMap
                    + '}';
        }
    }

    /** DelayedProduceMetadata. */
    public static class DelayedBucketStatus<T extends WriteResultForBucket> {
        private final long requiredOffset;
        private final T writeResultForBucket;
        /** Whether this bucket is waiting acks. */
        private volatile boolean acksPending;
        /** The error code of the delayed operation. */
        private volatile Errors delayedError;

        public DelayedBucketStatus(long requiredOffset, T writeResultForBucket) {
            this.requiredOffset = requiredOffset;
            this.writeResultForBucket = writeResultForBucket;
            this.acksPending = false;
            this.delayedError = null;
        }

        public long getRequiredOffset() {
            return requiredOffset;
        }

        public T getWriteResultForBucket() {
            return writeResultForBucket;
        }

        public boolean isAcksPending() {
            return acksPending;
        }

        public Errors getDelayedError() {
            return delayedError;
        }

        public void setAcksPending(boolean acksPending) {
            this.acksPending = acksPending;
        }

        public void setDelayedError(Errors delayedError) {
            this.delayedError = delayedError;
        }

        @Override
        public String toString() {
            return "DelayedBucketStatus{"
                    + "requiredOffset="
                    + requiredOffset
                    + ", writeResultForBucket="
                    + writeResultForBucket
                    + ", acksPending="
                    + acksPending
                    + '}';
        }
    }
}
