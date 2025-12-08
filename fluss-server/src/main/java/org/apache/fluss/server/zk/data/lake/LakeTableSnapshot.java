/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.zk.data.lake;

import org.apache.fluss.metadata.TableBucket;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** The snapshot info for a table. */
public class LakeTableSnapshot {

    // the last committed snapshot id in lake
    private final long snapshotId;

    // the log offset of the bucket
    // mapping from bucket id to log end offset or max timestamp,
    // will be null if log offset is unknown such as reading the snapshot of primary key table
    private final Map<TableBucket, Long> bucketLogEndOffset;

    public LakeTableSnapshot(long snapshotId, Map<TableBucket, Long> bucketLogEndOffset) {
        this.snapshotId = snapshotId;
        this.bucketLogEndOffset = bucketLogEndOffset;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public Optional<Long> getLogEndOffset(TableBucket tableBucket) {
        return Optional.ofNullable(bucketLogEndOffset.get(tableBucket));
    }

    public Map<TableBucket, Long> getBucketLogEndOffset() {
        return bucketLogEndOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LakeTableSnapshot that = (LakeTableSnapshot) o;
        return snapshotId == that.snapshotId
                && Objects.equals(bucketLogEndOffset, that.bucketLogEndOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, bucketLogEndOffset);
    }

    @Override
    public String toString() {
        return "LakeTableSnapshot{"
                + "snapshotId="
                + snapshotId
                + ", bucketLogEndOffset="
                + bucketLogEndOffset
                + '}';
    }
}
