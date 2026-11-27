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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.rpc.messages.GetBulkLoadInfoResponse;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetProgress;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.fluss.client.utils.ClientRpcMessageUtils.toBulkLoadHandle;

/** The current status and target progress of a BulkLoad transaction. */
@PublicEvolving
public final class BulkLoadInfo {

    private final BulkLoadHandle handle;
    private final State state;
    private final long createdTime;
    private final @Nullable Long finishedTime;
    private final @Nullable String abortMessage;
    private final List<TargetProgress> targets;

    /** Constructs the current transaction status from the coordinator response. */
    @Internal
    public BulkLoadInfo(GetBulkLoadInfoResponse response) {
        PbBulkLoadStatus status = response.getStatus();
        handle = toBulkLoadHandle(status.getHandle());
        state = State.fromCode(status.getState());
        createdTime = status.getCreatedTime();
        finishedTime = status.hasFinishedTime() ? status.getFinishedTime() : null;
        abortMessage = status.hasAbortMessage() ? status.getAbortMessage() : null;
        targets =
                Collections.unmodifiableList(
                        response.getTargetsList().stream()
                                .map(TargetProgress::new)
                                .collect(Collectors.toList()));
    }

    /** Returns the transaction handle. */
    public BulkLoadHandle getHandle() {
        return handle;
    }

    /** Returns the transaction state. */
    public State getState() {
        return state;
    }

    /** Returns when the transaction began, in milliseconds since the Unix epoch. */
    public long getCreatedTime() {
        return createdTime;
    }

    /** Returns when the transaction finished, in milliseconds since the Unix epoch. */
    @Nullable
    public Long getFinishedTime() {
        return finishedTime;
    }

    /** Returns the diagnostic supplied when the transaction was aborted, if present. */
    @Nullable
    public String getAbortMessage() {
        return abortMessage;
    }

    /** Returns progress for the original targets, including withdrawn targets. */
    public List<TargetProgress> getTargets() {
        return targets;
    }

    /** The coordinator's transaction state. */
    @PublicEvolving
    public enum State {
        RUNNING,
        COMMITTED,
        ABORTED;

        private static State fromCode(int code) {
            switch (code) {
                case 0:
                    return RUNNING;
                case 1:
                    return COMMITTED;
                case 2:
                    return ABORTED;
                default:
                    throw new IllegalArgumentException("Unknown BulkLoad state: " + code);
            }
        }
    }

    /** Current bucket progress for one physical BulkLoad target. */
    @PublicEvolving
    public static final class TargetProgress {

        private final @Nullable Long partitionId;
        private final int bucketCount;
        private final long allocatedBucketCount;
        private final long committedBucketCount;
        private final boolean withdrawn;

        private TargetProgress(PbBulkLoadTargetProgress progress) {
            partitionId = progress.hasPartitionId() ? progress.getPartitionId() : null;
            bucketCount = progress.getBucketCount();
            allocatedBucketCount = progress.getAllocatedBucketCount();
            committedBucketCount = progress.getCommittedBucketCount();
            withdrawn = progress.isWithdrawn();
        }

        /** Returns the partition ID, or null for a non-partitioned table. */
        @Nullable
        public Long getPartitionId() {
            return partitionId;
        }

        /** Returns the number of buckets in the target. */
        public int getBucketCount() {
            return bucketCount;
        }

        /** Returns the number of allocated buckets, including committed buckets. */
        public long getAllocatedBucketCount() {
            return allocatedBucketCount;
        }

        /** Returns the number of buckets whose snapshots have been committed. */
        public long getCommittedBucketCount() {
            return committedBucketCount;
        }

        /**
         * Returns whether the physical target has been dropped and withdrawn from the transaction.
         */
        public boolean isWithdrawn() {
            return withdrawn;
        }
    }
}
