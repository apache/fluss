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

package com.alibaba.fluss.cluster;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

/** This is used to describe per-bucket location information. */
@Internal
public final class BucketLocation {
    private final PhysicalTablePath physicalTablePath;
    private final TableBucket tableBucket;
    @Nullable private final ServerNode leader;
    private final ServerNode[] replicas;

    // TODO add inSyncReplicas and offlineReplicas.

    public BucketLocation(
            PhysicalTablePath physicalTablePath,
            long tableId,
            int bucketId,
            @Nullable ServerNode leader,
            ServerNode[] replicas) {
        this(physicalTablePath, new TableBucket(tableId, bucketId), leader, replicas);
    }

    public BucketLocation(
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            @Nullable ServerNode leader,
            ServerNode[] replicas) {
        this.physicalTablePath = physicalTablePath;
        this.tableBucket = tableBucket;
        this.leader = leader;
        this.replicas = replicas;
    }

    public PhysicalTablePath getPhysicalTablePath() {
        return physicalTablePath;
    }

    public int getBucketId() {
        return tableBucket.getBucket();
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    @Nullable
    public ServerNode getLeader() {
        return leader;
    }

    public ServerNode[] getReplicas() {
        return replicas;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof BucketLocation)) {
            return false;
        }
        BucketLocation that = (BucketLocation) object;
        return Objects.equals(physicalTablePath, that.physicalTablePath)
                && Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(leader, that.leader)
                && Objects.deepEquals(replicas, that.replicas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(physicalTablePath, tableBucket, leader, Arrays.hashCode(replicas));
    }

    @Override
    public String toString() {
        return String.format(
                "Bucket(physicalTablePath = %s, %s, leader = %s, replicas = %s)",
                physicalTablePath,
                tableBucket,
                leader == null ? "none" : leader.id(),
                formatNodeIds(replicas));
    }

    /** Extract the node ids from each item in the array and format for display. */
    private static String formatNodeIds(ServerNode[] nodes) {
        StringBuilder b = new StringBuilder("[");
        if (nodes != null) {
            for (int i = 0; i < nodes.length; i++) {
                b.append(nodes[i].id());
                if (i < nodes.length - 1) {
                    b.append(',');
                }
            }
        }
        b.append("]");
        return b.toString();
    }
}
