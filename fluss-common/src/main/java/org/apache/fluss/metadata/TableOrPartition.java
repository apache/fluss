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

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * A class to identify a table or a partition, containing the table id and the optional partition
 * id.
 */
@Internal
public class TableOrPartition {

    @Nullable private final Long tableId;
    @Nullable private final Long partitionId;

    /** Create a {@link TableOrPartition} instance for a table. */
    public static TableOrPartition ofTable(long tableId) {
        return new TableOrPartition(tableId, null);
    }

    /** Create a {@link TableOrPartition} instance for a partition. */
    public static TableOrPartition ofPartition(long partitionId) {
        return new TableOrPartition(null, partitionId);
    }

    /**
     * Create a {@link TableOrPartition} instance for the given table id and optional partition id:
     * a partition when {@code partitionId} is non-null, otherwise the table itself. Note that a
     * partition is identified by its (globally unique) partition id alone, so the table id is not
     * retained in that case.
     */
    public static TableOrPartition of(long tableId, @Nullable Long partitionId) {
        return partitionId == null ? ofTable(tableId) : ofPartition(partitionId);
    }

    private TableOrPartition(@Nullable Long tableId, @Nullable Long partitionId) {
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    @Nullable
    public Long getTableId() {
        return tableId;
    }

    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableOrPartition that = (TableOrPartition) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(partitionId, that.partitionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, partitionId);
    }

    @Override
    public String toString() {
        return "TableOrPartition{" + "tableId=" + tableId + ", partitionId=" + partitionId + '}';
    }
}
