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

package com.alibaba.fluss.spark.initializer;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.admin.ListOffsetsResult;
import com.alibaba.fluss.client.admin.OffsetSpec;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.spark.exception.SparkRuntimeException;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.alibaba.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;

/** The default implementation for offsets retriever. */
public class BucketOffsetsRetrieverImpl implements OffsetsInitializer.BucketOffsetsRetriever {
    private final Admin flussAdmin;
    private final TablePath tablePath;

    public BucketOffsetsRetrieverImpl(Admin flussAdmin, TablePath tablePath) {
        this.flussAdmin = flussAdmin;
        this.tablePath = tablePath;
    }

    @Override
    public Map<Integer, Long> latestOffsets(
            @Nullable String partitionName, Collection<Integer> buckets) {
        return listOffsets(partitionName, buckets, new OffsetSpec.LatestSpec());
    }

    @Override
    public Map<Integer, Long> earliestOffsets(
            @Nullable String partitionName, Collection<Integer> buckets) {
        Map<Integer, Long> bucketWithOffset = new HashMap<>();
        for (Integer bucket : buckets) {
            bucketWithOffset.put(bucket, EARLIEST_OFFSET);
        }
        return bucketWithOffset;
    }

    @Override
    public Map<Integer, Long> offsetsFromTimestamp(
            @Nullable String partitionName, Collection<Integer> buckets, long timestamp) {
        return listOffsets(partitionName, buckets, new OffsetSpec.TimestampSpec(timestamp));
    }

    private Map<Integer, Long> listOffsets(
            @Nullable String partitionName, Collection<Integer> buckets, OffsetSpec offsetSpec) {
        try {
            final ListOffsetsResult result;
            if (partitionName == null) {
                result = flussAdmin.listOffsets(tablePath, buckets, offsetSpec);
            } else {
                result = flussAdmin.listOffsets(tablePath, partitionName, buckets, offsetSpec);
            }
            return result.all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SparkRuntimeException(
                    "Interrupted while listing offsets for table buckets: " + buckets, e);
        } catch (ExecutionException e) {
            throw new SparkRuntimeException(
                    "Failed to list offsets for table buckets: " + buckets + " due to", e);
        }
    }
}
