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
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.apache.fluss.trino.FlussTableScanValidator.validateSupportedTable;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Plans fixed bucket ranges for nonpartitioned, native Fluss log tables. */
public final class FlussSplitManager implements ConnectorSplitManager {
    private final FlussMetadataAccess metadataAccess;

    @Inject
    FlussSplitManager(FlussMetadataAccess metadataAccess) {
        this.metadataAccess = checkNotNull(metadataAccess, "metadataAccess is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint) {
        FlussTableHandle handle = (FlussTableHandle) table;
        validateSupportedTable(metadataAccess.getTableInfo(handle));

        ImmutableList.Builder<Integer> buckets = ImmutableList.builder();
        for (int bucket = 0; bucket < handle.getBucketCount(); bucket++) {
            buckets.add(bucket);
        }
        List<Integer> allBuckets = buckets.build();
        Map<Integer, Long> starts =
                metadataAccess.listOffsets(handle, allBuckets, new OffsetSpec.EarliestSpec());
        Map<Integer, Long> stops =
                metadataAccess.listOffsets(handle, allBuckets, new OffsetSpec.LatestSpec());

        // Revalidate table identity and topology after capturing offset boundaries.
        // identity check happened in getTableInfo()
        validateSupportedTable(metadataAccess.getTableInfo(handle));

        ImmutableList.Builder<FlussSplit> splits = ImmutableList.builder();
        for (int bucket : allBuckets) {
            long start = starts.get(bucket);
            long stop = stops.get(bucket);
            if (start > stop) {
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR,
                        "Invalid Fluss offset range for "
                                + handle
                                + " bucket "
                                + bucket
                                + ": ["
                                + start
                                + ", "
                                + stop
                                + ")");
            }
            if (start < stop) {
                splits.add(new FlussSplit(bucket, start, stop));
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
