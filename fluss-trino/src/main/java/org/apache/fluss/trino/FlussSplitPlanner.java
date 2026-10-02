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
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.apache.fluss.trino.FlussErrorCode.FLUSS_SPLIT_ERROR;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

final class FlussSplitPlanner {
    private final FlussMetadataAccess metadataAccess;

    @Inject
    FlussSplitPlanner(FlussMetadataAccess metadataAccess) {
        this.metadataAccess = checkNotNull(metadataAccess, "metadataAccess is null");
    }

    List<FlussSplit> plan(FlussTableHandle table, TableInfo tableInfo) {
        List<FlussPhysicalBucket> buckets = metadataAccess.listScanBuckets(table);

        validateBucketShape(buckets, tableInfo);

        if (buckets.isEmpty()) {
            return ImmutableList.of();
        }

        if (tableInfo.hasPrimaryKey()) {
            return planKvSplits(buckets);
        }

        return planLogSplits(table, buckets);
    }

    private static void validateBucketShape(
            List<FlussPhysicalBucket> buckets, TableInfo tableInfo) {
        Set<FlussBucketHandle> seen = new HashSet<>();

        for (FlussPhysicalBucket physicalBucket : buckets) {
            FlussBucketHandle bucket = physicalBucket.getBucket();

            if (bucket.isPartitioned() != tableInfo.isPartitioned()) {
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR,
                        "Fluss bucket partition layout does not match table metadata: "
                                + bucket
                                + ", tablePartitioned="
                                + tableInfo.isPartitioned());
            }

            if (!seen.add(bucket)) {
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR,
                        "Duplicate Fluss bucket in scan planning: " + bucket);
            }
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

        Map<FlussBucketHandle, Long> stops =
                metadataAccess.resolveOffsets(table, buckets, new OffsetSpec.LatestSpec());

        metadataAccess.validateCurrentBuckets(table, buckets);

        ImmutableList.Builder<FlussSplit> splits = ImmutableList.builder();

        for (FlussPhysicalBucket physicalBucket : buckets) {
            FlussBucketHandle bucket = physicalBucket.getBucket();

            long start = starts.get(bucket);
            long stop = stops.get(bucket);

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
