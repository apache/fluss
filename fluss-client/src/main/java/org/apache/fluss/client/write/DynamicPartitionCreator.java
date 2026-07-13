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

package org.apache.fluss.client.write;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.apache.fluss.utils.ExceptionUtils.stripCompletionException;
import static org.apache.fluss.utils.PartitionUtils.validateAutoPartitionTime;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** A creator to create partition when dynamic partition create enable for table. */
@ThreadSafe
public class DynamicPartitionCreator {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionCreator.class);

    private final MetadataUpdater metadataUpdater;
    private final boolean dynamicPartitionEnabled;
    private final Admin admin;
    private final Consumer<Throwable> fatalErrorHandler;

    private final Set<PhysicalTablePath> inflightPartitionsToCreate = ConcurrentHashMap.newKeySet();
    private final Map<PhysicalTablePath, Throwable> partitionCreationFailures =
            new ConcurrentHashMap<>();
    private final Duration metadataWaitTimeout;

    public DynamicPartitionCreator(
            MetadataUpdater metadataUpdater,
            Admin admin,
            boolean dynamicPartitionEnabled,
            Duration metadataWaitTimeout,
            Consumer<Throwable> fatalErrorHandler) {
        this.metadataUpdater = metadataUpdater;
        this.admin = admin;
        this.dynamicPartitionEnabled = dynamicPartitionEnabled;
        this.metadataWaitTimeout = metadataWaitTimeout;
        this.fatalErrorHandler = fatalErrorHandler;
    }

    /**
     * Ensures the partition of the given path exists and its metadata (partition id and actual
     * bucket count) is present in the client cluster metadata before returning, creating the
     * partition dynamically if enabled. Bucket assignment must never run before the partition
     * metadata is available: the partition's actual bucket count is only known from the partition
     * metadata, and routing by any other value can silently place records in the wrong bucket.
     */
    public void checkAndCreatePartition(PhysicalTablePath physicalTablePath, TableInfo tableInfo) {
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
                // another thread is creating the partition; wait for its metadata below.
                LOG.debug("Partition {} is already being created, waiting.", physicalTablePath);
            } else if (forceCheckPartitionExist(physicalTablePath)) {
                // the partition exists and its metadata has been refreshed synchronously.
                LOG.debug("Partition {} already exists, skipping.", physicalTablePath);
                return;
            } else {
                // Validate early, before touching any state. The strategy is only resolved here,
                // on the partition-creation path, not on the common "already exists" path.
                List<String> partitionKeys = tableInfo.getPartitionKeys();
                AutoPartitionStrategy autoPartitionStrategy =
                        tableInfo.getTableConfig().getAutoPartitionStrategy();
                ResolvedPartitionSpec resolvedPartitionSpec =
                        ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);
                validateAutoPartitionTime(
                        resolvedPartitionSpec.toPartitionSpec(),
                        partitionKeys,
                        autoPartitionStrategy);

                // create partition if not exists.
                // partition may not exist, we should try to create it.
                if (inflightPartitionsToCreate.add(physicalTablePath)) {
                    // if the partition is not in inflightPartitionsToCreate, we should create it.
                    // this means that the partition is not being created by other threads.
                    LOG.info("Dynamically creating partition for {}", physicalTablePath);
                    partitionCreationFailures.remove(physicalTablePath);
                    createPartition(physicalTablePath, partitionKeys);
                } else {
                    LOG.debug("Partition {} is already being created, waiting.", physicalTablePath);
                }
            }
            waitForPartitionMetadata(physicalTablePath);
        }
    }

    /**
     * Polls with backoff until the partition metadata is visible in the client cluster metadata, or
     * fails when the creation failed or the bounded wait times out.
     */
    private void waitForPartitionMetadata(PhysicalTablePath physicalTablePath) {
        long deadlineNanos = System.nanoTime() + metadataWaitTimeout.toNanos();
        long backoffMs = 100;
        while (true) {
            Throwable creationFailure = partitionCreationFailures.remove(physicalTablePath);
            if (creationFailure != null) {
                throw new FlussRuntimeException(
                        "Failed to dynamically create partition " + physicalTablePath,
                        creationFailure);
            }
            if (metadataUpdater.getPartitionId(physicalTablePath).isPresent()
                    || forceCheckPartitionExist(physicalTablePath)) {
                return;
            }
            if (System.nanoTime() >= deadlineNanos) {
                throw new FlussRuntimeException(
                        String.format(
                                "Timed out after %s waiting for metadata of partition %s. The "
                                        + "record is not written; retry once the partition "
                                        + "metadata is available.",
                                metadataWaitTimeout, physicalTablePath));
            }
            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlussRuntimeException(
                        "Interrupted while waiting for metadata of partition " + physicalTablePath,
                        e);
            }
            backoffMs = Math.min(backoffMs * 2, 1000);
        }
    }

    private boolean forceCheckPartitionExist(PhysicalTablePath physicalTablePath) {
        boolean idExist = false;
        // force an IO to check whether the partition exists
        try {
            idExist = metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (t instanceof PartitionNotExistException) {
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

    private void createPartition(PhysicalTablePath physicalTablePath, List<String> partitionKeys) {
        String partitionName = physicalTablePath.getPartitionName();
        TablePath tablePath = physicalTablePath.getTablePath();
        checkArgument(partitionName != null, "Partition name shouldn't be null.");
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
        // waiters in waitForPartitionMetadata poll and refresh the metadata themselves.
        LOG.info("Successfully created partition {}", physicalTablePath);
    }

    private void onPartitionCreationFailed(
            PhysicalTablePath physicalTablePath, Throwable throwable) {
        partitionCreationFailures.put(physicalTablePath, stripCompletionException(throwable));
        inflightPartitionsToCreate.remove(physicalTablePath);
        fatalErrorHandler.accept(
                new FlussRuntimeException(
                        "Failed to dynamically create partition " + physicalTablePath,
                        stripCompletionException(throwable)));
    }
}
