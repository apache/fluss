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

package org.apache.fluss.lake.committer;

import org.apache.fluss.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Objects;

/**
 * A candidate partition passed to {@link PartitionDoneHandler} for mark-done judgement. Transient,
 * runtime-only input keyed by {@code partitionName} (not to be confused with the persisted {@link
 * LakeTieringTableState} keyed by partitionId).
 *
 * <p>{@code lastUpdateTime} is the aggregated MAX of bucket max-timestamps (the partition's last
 * write time in processing-time mode); -1 means unknown.
 *
 * @since 0.9
 */
@PublicEvolving
public class PartitionDoneCandidate implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String partitionName;
    private final long lastUpdateTime;

    public PartitionDoneCandidate(String partitionName, long lastUpdateTime) {
        this.partitionName = partitionName;
        this.lastUpdateTime = lastUpdateTime;
    }

    public String partitionName() {
        return partitionName;
    }

    public long lastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionDoneCandidate that = (PartitionDoneCandidate) o;
        return lastUpdateTime == that.lastUpdateTime
                && Objects.equals(partitionName, that.partitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionName, lastUpdateTime);
    }

    @Override
    public String toString() {
        return "PartitionDoneCandidate{"
                + "partitionName='"
                + partitionName
                + '\''
                + ", lastUpdateTime="
                + lastUpdateTime
                + '}';
    }
}
