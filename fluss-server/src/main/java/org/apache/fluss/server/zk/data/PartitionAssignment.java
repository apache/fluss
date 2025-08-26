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

package org.apache.fluss.server.zk.data;

import java.util.Map;
import java.util.Objects;

/**
 * The assignment of a partition which is consisted with replica assignment of each bucket. The
 * assignment is represented as a list of integers where each integer is the replica ID (server id).
 *
 * <p>Different from {@link TableAssignment}, it contains a {@link #tableId} to use to know which
 * table this partition belongs to.
 *
 * <p>The assignment information is stored in {@link ZkData.PartitionIdZNode}.
 *
 * @see PartitionAssignmentJsonSerde for json serialization and deserialization.
 */
public class PartitionAssignment extends TableAssignment {

    private final long tableId;

    public PartitionAssignment(long tableId, Map<Integer, BucketAssignment> assignments) {
        super(assignments);
        this.tableId = tableId;
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PartitionAssignment that = (PartitionAssignment) o;
        return tableId == that.tableId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), tableId);
    }
}
