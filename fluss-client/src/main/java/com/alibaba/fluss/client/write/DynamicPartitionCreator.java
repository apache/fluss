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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/** A creator to create partition when dynamic partition create enable for table. */
public class DynamicPartitionCreator {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionCreator.class);

    private final MetadataUpdater metadataUpdater;
    private final Admin admin;

    private final Object inflightPartitionsLock = new Object();

    @GuardedBy("inflightPartitionsLock")
    private final Set<PhysicalTablePath> inflightPartitionsToCreate = new HashSet<>();

    private volatile Throwable cachedCreatePartitionException = null;

    public DynamicPartitionCreator(MetadataUpdater metadataUpdater, Admin admin) {
        this.metadataUpdater = metadataUpdater;
        this.admin = admin;
    }

    public void createPartitionIfNeed(PhysicalTablePath physicalTablePath) {
        if (cachedCreatePartitionException != null) {
            throw new FlussRuntimeException(cachedCreatePartitionException);
        }

        TableInfo tableInfo =
                metadataUpdater.getTableInfoOrElseThrow(physicalTablePath.getTablePath());
        if (!tableInfo.isPartitioned()) {
            return;
        }

        boolean dynamicPartitionEnabled = tableInfo.getTableConfig().isDynamicPartitionEnabled();
        Optional<Long> partitionIdOpt = metadataUpdater.getPartitionId(physicalTablePath);
        // first try to update metadata info if not exists.
        boolean isExists = partitionIdOpt.isPresent();
        if (!partitionIdOpt.isPresent()) {
            try {
                isExists = metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
            } catch (Exception e) {
                Throwable t = ExceptionUtils.stripExecutionException(e);
                if (t.getCause() instanceof PartitionNotExistException) {
                    if (!dynamicPartitionEnabled) {
                        throw new PartitionNotExistException(
                                String.format(
                                        "Table partition '%s' does not exist.", physicalTablePath));
                    }
                } else {
                    throw new FlussRuntimeException(e);
                }
            }
        }

        synchronized (inflightPartitionsLock) {
            if (isExists) {
                if (dynamicPartitionEnabled) {
                    inflightPartitionsToCreate.remove(physicalTablePath);
                }
            } else {
                if (!inflightPartitionsToCreate.contains(physicalTablePath)) {
                    inflightPartitionsToCreate.add(physicalTablePath);
                    createPartition(physicalTablePath);
                }
            }
        }
    }

    private void createPartition(PhysicalTablePath physicalTablePath) {
        String partitionName = physicalTablePath.getPartitionName();
        TablePath tablePath = physicalTablePath.getTablePath();
        checkArgument(partitionName != null, "Partition name shouldn't be null.");
        TableInfo tableInfo = metadataUpdater.getTableInfoOrElseThrow(tablePath);
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        ResolvedPartitionSpec resolvedPartitionSpec =
                ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);

        admin.createPartition(tablePath, resolvedPartitionSpec.toPartitionSpec(), true)
                .whenComplete(
                        (partitionId, throwable) -> {
                            if (throwable != null) {
                                // If encounter TooManyPartitionsException or
                                // TooManyBucketsException, we should set
                                // cachedCreatePartitionException to make the next createPartition
                                // call failed.
                                LOG.error(
                                        "Failed to dynamic create partition for {}",
                                        physicalTablePath,
                                        throwable);
                                cachedCreatePartitionException = throwable;
                            }
                        });
    }
}
