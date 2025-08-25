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

package org.apache.fluss.flink.source.state;

import org.apache.fluss.metadata.TableBucket;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** A checkpoint of the current state of the containing the buckets that is already assigned. */
public class SourceEnumeratorState {

    /** buckets that have been assigned to readers. */
    private final Set<TableBucket> assignedBuckets;

    // partitions that have been assigned to readers.
    // mapping from partition id to partition name
    private final Map<Long, String> assignedPartitions;

    public SourceEnumeratorState(
            Set<TableBucket> assignedBuckets, Map<Long, String> assignedPartitions) {
        this.assignedBuckets = assignedBuckets;
        this.assignedPartitions = assignedPartitions;
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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceEnumeratorState that = (SourceEnumeratorState) o;
        return Objects.equals(assignedBuckets, that.assignedBuckets)
                && Objects.equals(assignedPartitions, that.assignedPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignedBuckets, assignedPartitions);
    }

    @Override
    public String toString() {
        return "SourceEnumeratorState{"
                + "assignedBuckets="
                + assignedBuckets
                + ", assignedPartitions="
                + assignedPartitions
                + '}';
    }
}
