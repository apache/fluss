/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.lakehouse;

import com.alibaba.fluss.client.lakehouse.paimon.PaimonBucketAssigner;
import com.alibaba.fluss.client.write.BucketAssigner;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/** A bucket assigner for table with data lake enabled. */
public class LakeTableBucketAssigner implements BucketAssigner {

    // the bucket extractor of bucket, fluss will use the the bucket
    // that paimon assign to align with paimon when data lake is enabled
    // todo: make it pluggable
    private final PaimonBucketAssigner paimonBucketAssigner;

    public LakeTableBucketAssigner(RowType rowType, List<String> bucketKey, int bucketNum) {
        this.paimonBucketAssigner = new PaimonBucketAssigner(rowType, bucketKey, bucketNum);
    }

    @Override
    public int assignBucket(@Nullable byte[] key, Cluster cluster) {
        // shouldn't come in here
        throw new UnsupportedOperationException(
                "Method assignBucket(byte[], Cluster) is not supported in "
                        + getClass().getSimpleName());
    }

    @Override
    public int assignBucket(@Nullable byte[] key, InternalRow row, Cluster cluster) {
        return paimonBucketAssigner.assignBucket(row);
    }

    @Override
    public boolean abortIfBatchFull() {
        return false;
    }

    @Override
    public void close() {
        // do nothing now.
    }
}
