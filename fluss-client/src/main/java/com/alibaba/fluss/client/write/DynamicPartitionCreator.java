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

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static com.alibaba.fluss.utils.ExceptionUtils.stripCompletionException;
import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/** A creator to create partition when dynamic partition create enable for table. */
@ThreadSafe
public class DynamicPartitionCreator {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionCreator.class);

    private final MetadataUpdater metadataUpdater;
    private final boolean dynamicPartitionEnabled;
    private final Admin admin;
    private final Consumer<Throwable> fatalErrorHandler;

    private final Set<PhysicalTablePath> inflightPartitionsToCreate = ConcurrentHashMap.newKeySet();

    public DynamicPartitionCreator(
            MetadataUpdater metadataUpdater,
            Admin admin,
            boolean dynamicPartitionEnabled,
            Consumer<Throwable> fatalErrorHandler) {
        this.metadataUpdater = metadataUpdater;
        this.admin = admin;
        this.dynamicPartitionEnabled = dynamicPartitionEnabled;
        this.fatalErrorHandler = fatalErrorHandler;
    }

    public void checkAndCreatePartitionAsync(PhysicalTablePath physicalTablePath) {
        String partitionName = physicalTablePath.getPartitionName();
        if (partitionName == null) {
            // no need to check and create partition
            return;
        }

        Optional<Long> partitionIdOpt = metadataUpdater.getPartitionId(physicalTablePath);
        // first try to update metadata info if not exists.
        boolean idExist = partitionIdOpt.isPresent();
        if (!idExist) {
            if (inflightPartitionsToCreate.contains(physicalTablePath)) {
                // if the partition is already in inflightPartitionsToCreate, we should skip
                // creating it.
                LOG.debug("Partition {} is already being created, skipping.", physicalTablePath);
            } else if (forceCheckPartitionExist(physicalTablePath)) {
                // if the partition exists, we should skip creating it.
                LOG.debug("Partition {} already exists, skipping.", physicalTablePath);
            } else {
                // create partition if not exists.
                // partition may not exist, we should try to create it.
                if (inflightPartitionsToCreate.add(physicalTablePath)) {
                    // if the partition is not in inflightPartitionsToCreate, we should create it.
                    // this means that the partition is not being created by other threads.
                    LOG.info("Dynamically creating partition partition for {}", physicalTablePath);
                    createPartition(physicalTablePath);
                } else {
                    // if the partition is already in inflightPartitionsToCreate, we should skip
                    // creating it.
                    LOG.debug(
                            "Partition {} is already being created, skipping.", physicalTablePath);
                }
            }
        }
    }

    private boolean forceCheckPartitionExist(PhysicalTablePath physicalTablePath) {
        boolean idExist = false;
        // force an IO to check whether the partition exists
        try {
            // force an IO to check whether the partition exists
            idExist = metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (t.getCause() instanceof PartitionNotExistException) {
                if (!dynamicPartitionEnabled) {
                    throw new PartitionNotExistException(
                            String.format(
                                    "Table partition '%s' does not exist.", physicalTablePath));
                }
            } else {
                throw new FlussRuntimeException(e.getMessage(), e);
            }
        }
        return idExist;
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
                        (ignore, throwable) -> {
                            if (throwable != null) {
                                // If encounter TooManyPartitionsException or
                                // TooManyBucketsException, we should set
                                // cachedCreatePartitionException to make the next createPartition
                                // call failed.
                                onPartitionCreationFailed(physicalTablePath, throwable);
                            } else {
                                onPartitionCreationSuccess(physicalTablePath);
                            }
                        });
    }

    private void onPartitionCreationSuccess(PhysicalTablePath physicalTablePath) {
        inflightPartitionsToCreate.remove(physicalTablePath);
        // TODO: trigger to update metadata here when metadataUpdater supports async update
        // metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
        LOG.info("Successfully created partition {}", physicalTablePath);
    }

    private void onPartitionCreationFailed(
            PhysicalTablePath physicalTablePath, Throwable throwable) {
        inflightPartitionsToCreate.remove(physicalTablePath);
        fatalErrorHandler.accept(
                new FlussRuntimeException(
                        "Failed to dynamically create partition " + physicalTablePath,
                        stripCompletionException(throwable)));
    }
}
