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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

import java.util.Collections;
import java.util.Map;

/**
 * A class representing the lake snapshot information of a table. It contains:
 * <li>The snapshot id and the log offset for each bucket.
 *
 * @since 0.3
 */
@PublicEvolving
public class LakeSnapshot {

    private final long snapshotId;

    // the specific log offset of the snapshot
    private final Map<TableBucket, Long> tableBucketsOffset;

    // the partition name by partition id of this lake snapshot if
    // is a partitioned table, empty if not a partitioned table
    private final Map<Long, String> partitionNameById;

    public LakeSnapshot(
            long snapshotId,
            Map<TableBucket, Long> tableBucketsOffset,
            Map<Long, String> partitionNameById) {
        this.snapshotId = snapshotId;
        this.tableBucketsOffset = tableBucketsOffset;
        this.partitionNameById = partitionNameById;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public Map<TableBucket, Long> getTableBucketsOffset() {
        return Collections.unmodifiableMap(tableBucketsOffset);
    }

    public Map<Long, String> getPartitionNameById() {
        return Collections.unmodifiableMap(partitionNameById);
    }

    @Override
    public String toString() {
        return "LakeSnapshot{"
                + "snapshotId="
                + snapshotId
                + ", tableBucketsOffset="
                + tableBucketsOffset
                + ", partitionNameById="
                + partitionNameById
                + '}';
    }
}
