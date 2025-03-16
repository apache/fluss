/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.source.state;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** A checkpoint of the current state of the containing the buckets that is already assigned. */
public class SourceEnumeratorState {

    /** tables that have been assigned to readers. */
    // mapping from table id to table path
    private final Map<Long, TablePath> assignedTables;

    /** buckets that have been assigned to readers. */
    private final Set<TableBucket> assignedBuckets;

    // partitions that have been assigned to readers.
    // mapping from partition id to partition name
    private final Map<Long, String> assignedPartitions;

    public SourceEnumeratorState(
            Map<Long, TablePath> assignedTables,
            Set<TableBucket> assignedBuckets,
            Map<Long, String> assignedPartitions) {
        this.assignedTables = assignedTables;
        this.assignedBuckets = assignedBuckets;
        this.assignedPartitions = assignedPartitions;
    }

    public Map<Long, TablePath> getAssignedTables() {
        return assignedTables;
    }

    public Set<TableBucket> getAssignedBuckets() {
        return assignedBuckets;
    }

    public Map<Long, String> getAssignedPartitions() {
        return assignedPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SourceEnumeratorState)) {
            return false;
        }
        SourceEnumeratorState that = (SourceEnumeratorState) o;
        return Objects.equals(assignedTables, that.assignedTables)
                && Objects.equals(assignedBuckets, that.assignedBuckets)
                && Objects.equals(assignedPartitions, that.assignedPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignedTables, assignedBuckets, assignedPartitions);
    }

    @Override
    public String toString() {
        return "SourceEnumeratorState{"
                + "assignedTables="
                + assignedTables
                + ", assignedBuckets="
                + assignedBuckets
                + ", assignedPartitions="
                + assignedPartitions
                + '}';
    }
}
