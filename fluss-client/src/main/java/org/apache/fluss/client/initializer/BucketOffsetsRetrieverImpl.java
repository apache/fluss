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

package org.apache.fluss.client.initializer;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetMetadata;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;

/** The default implementation for offsets retriever. */
public class BucketOffsetsRetrieverImpl implements OffsetsInitializer.BucketOffsetsRetriever {
    private final Admin flussAdmin;
    private final TablePath tablePath;
    private final boolean fetchEarliestOffset;

    public BucketOffsetsRetrieverImpl(Admin flussAdmin, TablePath tablePath) {
        this(flussAdmin, tablePath, false);
    }

    public BucketOffsetsRetrieverImpl(
            Admin flussAdmin, TablePath tablePath, boolean fetchEarliestOffset) {
        this.flussAdmin = flussAdmin;
        this.tablePath = tablePath;
        this.fetchEarliestOffset = fetchEarliestOffset;
    }

    @Override
    public Map<Integer, Long> latestOffsets(
            @Nullable String partitionName, Collection<Integer> buckets) {
        return listOffsets(partitionName, buckets, new OffsetSpec.LatestSpec());
    }

    @Override
    public Map<Integer, Long> earliestOffsets(
            @Nullable String partitionName, Collection<Integer> buckets) {
        if (!fetchEarliestOffset) {
            Map<Integer, Long> bucketWithOffset = new HashMap<>(buckets.size());
            for (Integer bucket : buckets) {
                bucketWithOffset.put(bucket, EARLIEST_OFFSET);
            }
            return bucketWithOffset;
        } else {
            return listOffsets(partitionName, buckets, new OffsetSpec.EarliestSpec());
        }
    }

    @Override
    public Map<Integer, Long> offsetsFromTimestamp(
            @Nullable String partitionName, Collection<Integer> buckets, long timestamp) {
        return listOffsets(partitionName, buckets, new OffsetSpec.TimestampSpec(timestamp));
    }

    @Override
    public Map<Integer, Long> committedOffsets(
            String groupId, @Nullable String partitionName, Collection<Integer> buckets) {
        return listConsumerOffsets(groupId, partitionName, buckets);
    }

    private Map<Integer, Long> listConsumerOffsets(
            String groupId, @Nullable String partitionName, Collection<Integer> buckets) {
        try {
            TableInfo tableInfo = flussAdmin.getTableInfo(tablePath).get();
            long tableId = tableInfo.getTableId();
            Long partitionId = null;
            if (partitionName != null) {
                ResolvedPartitionSpec partitionSpec =
                        ResolvedPartitionSpec.fromPartitionName(
                                tableInfo.getPartitionKeys(), partitionName);
                partitionId =
                        flussAdmin
                                .listPartitionInfos(tablePath, partitionSpec.toPartitionSpec())
                                .get()
                                .stream()
                                .filter(p -> p.getPartitionName().equals(partitionName))
                                .map(PartitionInfo::getPartitionId)
                                .findFirst()
                                .orElseThrow(
                                        () ->
                                                new FlussRuntimeException(
                                                        "Partition not found: "
                                                                + tablePath
                                                                + "/"
                                                                + partitionName));
            }
            Set<TableBucket> keys = new HashSet<>(buckets.size());
            for (Integer bucket : buckets) {
                keys.add(new TableBucket(tableId, partitionId, bucket));
            }
            Map<TableBucket, OffsetMetadata> committed =
                    flussAdmin.listConsumerGroupOffsets(groupId, keys).get();
            Map<Integer, Long> bucketOffsets = new HashMap<>(committed.size());
            for (Map.Entry<TableBucket, OffsetMetadata> entry : committed.entrySet()) {
                bucketOffsets.put(entry.getKey().getBucket(), entry.getValue().getOffset());
            }
            return bucketOffsets;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException(
                    "Interrupted while listing consumer offsets for group "
                            + groupId
                            + " and buckets: "
                            + buckets,
                    e);
        } catch (ExecutionException e) {
            throw new FlussRuntimeException(
                    "Failed to list consumer offsets for group "
                            + groupId
                            + " and buckets: "
                            + buckets
                            + " due to",
                    ExceptionUtils.stripExecutionException(e));
        }
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
            throw new FlussRuntimeException(
                    "Interrupted while listing offsets for table buckets: " + buckets, e);
        } catch (ExecutionException e) {
            throw new FlussRuntimeException(
                    "Failed to list offsets for table buckets: " + buckets + " due to",
                    ExceptionUtils.stripExecutionException(e));
        }
    }
}
