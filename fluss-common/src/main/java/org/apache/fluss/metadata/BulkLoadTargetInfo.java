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

import java.util.Arrays;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable input-generation contract frozen by a successful BulkLoad Begin request. */
@Internal
public final class BulkLoadTargetInfo {

    private final BulkLoadHandle handle;
    private final TableInfo tableInfo;
    private final long[] snapshotIds;

    /** Creates the immutable input-generation contract returned by BulkLoad Begin. */
    public BulkLoadTargetInfo(BulkLoadHandle handle, TableInfo tableInfo, long[] snapshotIds) {
        this.handle = checkNotNull(handle, "BulkLoad handle must not be null.");
        this.tableInfo = checkNotNull(tableInfo, "BulkLoad table info must not be null.");
        checkArgument(
                handle.getTarget().getTablePath().equals(tableInfo.getTablePath())
                        && handle.getTableId() == tableInfo.getTableId(),
                "BulkLoad handle and table info must identify the same physical table.");
        this.snapshotIds = validateSnapshotIds(snapshotIds, tableInfo.getNumBuckets());
    }

    /** Returns the transaction handle. */
    public BulkLoadHandle getHandle() {
        return handle;
    }

    /** Returns the complete table information frozen by Begin. */
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    /** Returns the allocated ordinary Snapshot ID for the given bucket. */
    public long getSnapshotId(int bucketId) {
        checkArgument(
                bucketId >= 0 && bucketId < snapshotIds.length,
                "BulkLoad bucket ID %s is out of range [0, %s).",
                bucketId,
                snapshotIds.length);
        return snapshotIds[bucketId];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BulkLoadTargetInfo that = (BulkLoadTargetInfo) o;
        return Objects.equals(handle, that.handle)
                && Objects.equals(tableInfo, that.tableInfo)
                && Arrays.equals(snapshotIds, that.snapshotIds);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(handle, tableInfo);
        result = 31 * result + Arrays.hashCode(snapshotIds);
        return result;
    }

    @Override
    public String toString() {
        return "BulkLoadTargetInfo{"
                + "handle="
                + handle
                + ", tableInfo="
                + tableInfo
                + ", snapshotIds="
                + Arrays.toString(snapshotIds)
                + '}';
    }

    private static long[] validateSnapshotIds(long[] snapshotIds, int numBuckets) {
        checkNotNull(snapshotIds, "BulkLoad Snapshot IDs must not be null.");
        checkArgument(
                snapshotIds.length == numBuckets,
                "BulkLoad Snapshot IDs must exactly cover [0, %s).",
                numBuckets);
        long[] copy = snapshotIds.clone();
        for (long snapshotId : copy) {
            checkArgument(snapshotId >= 0, "BulkLoad Snapshot ID must be non-negative.");
        }
        return copy;
    }
}
