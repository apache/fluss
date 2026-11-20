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

package org.apache.fluss.trino;

import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.trino.FlussErrorCode.FLUSS_SPLIT_ERROR;
import static org.apache.fluss.trino.FlussPartitionPruner.extractPartitionSpec;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

final class FlussSplitPlanner {
    private final FlussMetadataAccess metadataAccess;

    @Inject
    FlussSplitPlanner(FlussMetadataAccess metadataAccess) {
        this.metadataAccess = checkNotNull(metadataAccess, "metadataAccess is null");
    }

    List<FlussSplit> plan(FlussTableHandle table, TableInfo tableInfo) {
        Optional<PartitionSpec> partitionSpec =
                extractPartitionSpec(tableInfo, table.getConstraint());
        Optional<FlussBucketPruner> pruner =
                FlussBucketPruner.create(tableInfo, table.getConstraint());
        ImmutableList.Builder<FlussPhysicalBucket> buckets = ImmutableList.builder();
        if (tableInfo.isPartitioned()) {
            for (PartitionInfo partition :
                    metadataAccess.listScanPartitions(table, partitionSpec)) {
                addBuckets(
                        buckets,
                        tableInfo.getTableId(),
                        Optional.of(partition.getPartitionId()),
                        Optional.of(partition.getPartitionName()),
                        partition.getBucketCount(),
                        pruner);
            }
        } else {
            addBuckets(
                    buckets,
                    tableInfo.getTableId(),
                    Optional.empty(),
                    Optional.empty(),
                    tableInfo.getNumBuckets(),
                    pruner);
        }
        List<FlussPhysicalBucket> selected = buckets.build();
        if (tableInfo.hasPrimaryKey()) {
            return planKvSplits(selected);
        }
        return planLogSplits(table, selected);
    }

    private static void addBuckets(
            ImmutableList.Builder<FlussPhysicalBucket> buckets,
            long tableId,
            Optional<Long> partitionId,
            Optional<String> partitionName,
            int bucketCount,
            Optional<FlussBucketPruner> pruner) {
        if (bucketCount <= 0) {
            throw new TrinoException(FLUSS_SPLIT_ERROR, "Invalid bucket count: " + bucketCount);
        }
        int first = pruner.map(value -> value.getBucketId(bucketCount)).orElse(0);
        int end = pruner.isPresent() ? first + 1 : bucketCount;
        for (int bucketId = first; bucketId < end; bucketId++) {
            buckets.add(
                    new FlussPhysicalBucket(
                            new FlussBucketHandle(tableId, partitionId, bucketId), partitionName));
        }
    }

    private static List<FlussSplit> planKvSplits(List<FlussPhysicalBucket> buckets) {
        return buckets.stream()
                .map(FlussPhysicalBucket::getBucket)
                .map(FlussSplit::forKv)
                .collect(ImmutableList.toImmutableList());
    }

    private List<FlussSplit> planLogSplits(
            FlussTableHandle table, List<FlussPhysicalBucket> buckets) {
        Map<FlussBucketHandle, Long> starts =
                metadataAccess.resolveOffsets(table, buckets, new OffsetSpec.EarliestSpec());

        List<FlussPhysicalBucket> remainingBuckets =
                buckets.stream()
                        .filter(bucket -> starts.containsKey(bucket.getBucket()))
                        .collect(ImmutableList.toImmutableList());
        Map<FlussBucketHandle, Long> stops =
                metadataAccess.resolveOffsets(table, remainingBuckets, new OffsetSpec.LatestSpec());

        ImmutableList.Builder<FlussSplit> splits = ImmutableList.builder();

        for (FlussPhysicalBucket physicalBucket : remainingBuckets) {
            FlussBucketHandle bucket = physicalBucket.getBucket();

            Long start = starts.get(bucket);
            Long stop = stops.get(bucket);

            if (stop == null) {
                // The partition disappeared during the latest offset lookup.
                continue;
            }

            if (start > stop) {
                throw new TrinoException(
                        FLUSS_SPLIT_ERROR,
                        "Invalid Fluss log range for " + bucket + ": [" + start + "," + stop + ")");
            }

            if (start < stop) {
                splits.add(FlussSplit.forLog(bucket, start, stop));
            }
        }

        return splits.build();
    }
}
