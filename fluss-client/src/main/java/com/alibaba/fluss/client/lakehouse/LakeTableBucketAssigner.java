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
import com.alibaba.fluss.lakehouse.DataLakeType;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/** A bucket assigner for table with data lake enabled. */
public class LakeTableBucketAssigner implements BucketAssigner {

    private final AbstractBucketAssigner bucketAssigner;

    public LakeTableBucketAssigner(TableDescriptor tableDescriptor, int bucketNum) {
        DataLakeType dataLakeType = tableDescriptor.getDataLakeType();
        switch (dataLakeType) {
            case PAIMON:
                this.bucketAssigner =
                        new PaimonBucketAssigner(
                                tableDescriptor.getSchema().toRowType(),
                                tableDescriptor.getBucketKey(),
                                bucketNum);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Data lake type " + dataLakeType + " is not supported.");
        }
    }

    public LakeTableBucketAssigner(
            RowType rowType, List<String> bucketKey, int bucketNum, DataLakeType dataLakeType) {
        switch (dataLakeType) {
            case PAIMON:
                this.bucketAssigner = new PaimonBucketAssigner(rowType, bucketKey, bucketNum);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Data lake type " + dataLakeType + " is not supported.");
        }
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
        return bucketAssigner.assignBucket(row);
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
