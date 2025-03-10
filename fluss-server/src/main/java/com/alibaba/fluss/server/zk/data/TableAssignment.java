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

package com.alibaba.fluss.server.zk.data;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The assignment of a table which is consisted with replica assignment of each bucket. The
 * assignment is represented as a list of integers where each integer is the replica ID (server id).
 * The assignment information is stored in {@link ZkData.TableIdZNode}.
 *
 * @see TableAssignmentJsonSerde for json serialization and deserialization.
 */
public class TableAssignment {
    // a mapping from bucket id to assignment something like "{0: [0, 1, 3]}".
    // the assignment is represented as a list of replica id (tabletserver id) where the first
    // replica id will be considered as the replica leader.
    private final Map<Integer, BucketAssignment> assignments;

    public TableAssignment(Map<Integer, BucketAssignment> assignments) {
        this.assignments = assignments;
    }

    public Set<Integer> getBuckets() {
        return assignments.keySet();
    }

    public Map<Integer, BucketAssignment> getBucketAssignments() {
        return assignments;
    }

    public BucketAssignment getBucketAssignment(int bucketId) {
        return assignments.get(bucketId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableAssignment that = (TableAssignment) o;
        return Objects.equals(assignments, that.assignments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignments);
    }

    @Override
    public String toString() {
        return assignments.toString();
    }

    // ------------------------------------------------------------------------------------------

    public static Builder builder() {
        return new Builder();
    }

    /** A builder for creating a {@link TableAssignment}. */
    public static class Builder {
        private final Map<Integer, BucketAssignment> assignments;

        public Builder() {
            this.assignments = new HashMap<>();
        }

        public Builder add(int bucketId, BucketAssignment assignment) {
            assignments.put(bucketId, assignment);
            return this;
        }

        public TableAssignment build() {
            return new TableAssignment(assignments);
        }
    }
}
