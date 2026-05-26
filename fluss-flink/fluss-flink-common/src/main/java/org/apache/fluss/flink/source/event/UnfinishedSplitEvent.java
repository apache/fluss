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

package org.apache.fluss.flink.source.event;

import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.api.connector.source.SourceEvent;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Signal from a source reader back to the enumerator that a non-resumable split (today only {@link
 * org.apache.fluss.flink.source.split.KvBatchSplit}) could not be completed and must be
 * re-assigned. Sent on transient failures whose recovery requires opening a fresh scanner session,
 * most notably {@code NOT_LEADER_OR_FOLLOWER} / {@code LeaderNotAvailableException}.
 *
 * <p>The event carries the bucket and partition name so the enumerator can reconstruct the split
 * without keeping per-split state across its own restarts. The enumerator is expected to refresh
 * metadata and re-emit the split (possibly to a different reader), bounded by a per-split attempt
 * budget so a persistently failing bucket eventually fails the job rather than hot-looping.
 */
public class UnfinishedSplitEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final TableBucket tableBucket;
    @Nullable private final String partitionName;
    private final String reason;

    public UnfinishedSplitEvent(
            String splitId,
            TableBucket tableBucket,
            @Nullable String partitionName,
            String reason) {
        this.splitId = splitId;
        this.tableBucket = tableBucket;
        this.partitionName = partitionName;
        this.reason = reason;
    }

    public String getSplitId() {
        return splitId;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    @Nullable
    public String getPartitionName() {
        return partitionName;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnfinishedSplitEvent that = (UnfinishedSplitEvent) o;
        return Objects.equals(splitId, that.splitId)
                && Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(partitionName, that.partitionName)
                && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, tableBucket, partitionName, reason);
    }

    @Override
    public String toString() {
        return "UnfinishedSplitEvent{splitId='"
                + splitId
                + "', tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + "', reason='"
                + reason
                + "'}";
    }
}
