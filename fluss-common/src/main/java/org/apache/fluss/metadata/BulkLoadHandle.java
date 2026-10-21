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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.UUID;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable physical identity of a BulkLoad transaction. */
@PublicEvolving
public final class BulkLoadHandle {

    private final PhysicalTablePath target;
    private final long tableId;
    private final @Nullable Long partitionId;
    private final String bulkLoadId;

    /**
     * Creates a BulkLoad handle.
     *
     * @param target the original physical target path
     * @param tableId the physical table ID
     * @param partitionId the physical partition ID, or {@code null} for a non-partitioned target
     * @param bulkLoadId the canonical lowercase BulkLoad UUID
     */
    public BulkLoadHandle(
            PhysicalTablePath target, long tableId, @Nullable Long partitionId, String bulkLoadId) {
        this.target = checkNotNull(target, "BulkLoad target must not be null.");
        checkArgument(target.isValid(), "BulkLoad target must be valid.");
        checkArgument(tableId >= 0, "BulkLoad table ID must be non-negative.");
        checkArgument(
                partitionId == null || partitionId >= 0,
                "BulkLoad partition ID must be non-negative.");
        checkArgument(
                (target.getPartitionName() == null) == (partitionId == null),
                "BulkLoad partition name and partition ID must both be present or both be absent.");
        this.bulkLoadId = validateBulkLoadId(bulkLoadId);
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    /** Returns the original physical target path. */
    public PhysicalTablePath getTarget() {
        return target;
    }

    /** Returns the physical table ID. */
    public long getTableId() {
        return tableId;
    }

    /** Returns the physical partition ID, or {@code null} for a non-partitioned target. */
    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    /** Returns the canonical lowercase BulkLoad UUID. */
    public String getBulkLoadId() {
        return bulkLoadId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BulkLoadHandle that = (BulkLoadHandle) o;
        return tableId == that.tableId
                && Objects.equals(target, that.target)
                && Objects.equals(partitionId, that.partitionId)
                && Objects.equals(bulkLoadId, that.bulkLoadId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(target, tableId, partitionId, bulkLoadId);
    }

    @Override
    public String toString() {
        return "BulkLoadHandle{"
                + "target="
                + target
                + ", tableId="
                + tableId
                + ", partitionId="
                + partitionId
                + ", bulkLoadId='"
                + bulkLoadId
                + '\''
                + '}';
    }

    private static String validateBulkLoadId(String bulkLoadId) {
        checkNotNull(bulkLoadId, "BulkLoad ID must not be null.");
        try {
            checkArgument(
                    UUID.fromString(bulkLoadId).toString().equals(bulkLoadId),
                    "BulkLoad ID must be a canonical lowercase UUID.");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "BulkLoad ID must be a canonical lowercase UUID: " + bulkLoadId, e);
        }
        return bulkLoadId;
    }
}
