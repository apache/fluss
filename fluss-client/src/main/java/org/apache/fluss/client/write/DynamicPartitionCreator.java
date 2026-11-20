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
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
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
    private final PartitionCreationListener partitionCreationListener;

    private final Set<PhysicalTablePath> inflightPartitionsToCreate = ConcurrentHashMap.newKeySet();

    public DynamicPartitionCreator(
            MetadataUpdater metadataUpdater,
            Admin admin,
            boolean dynamicPartitionEnabled,
            Consumer<Throwable> fatalErrorHandler,
            PartitionCreationListener partitionCreationListener) {
        this.metadataUpdater = metadataUpdater;
        this.admin = admin;
        this.dynamicPartitionEnabled = dynamicPartitionEnabled;
        this.fatalErrorHandler = fatalErrorHandler;
        this.partitionCreationListener = partitionCreationListener;
    }

    /** Ensures the partition exists, creating it asynchronously when enabled. */
    public void checkAndCreatePartitionAsync(
            PhysicalTablePath physicalTablePath, TableInfo tableInfo) {
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
                createPartitionOnce(physicalTablePath, tableInfo);
            }
        }
    }

    /**
     * Creates a partition that refreshed metadata reports as missing while writes are queued for
     * it, e.g. because it was dropped after the writes were appended.
     *
     * <p>Unlike a new partition, the partition is only validated by the coordinator, as the
     * auto-partition settings of {@code tableInfo} may predate changes to the table. As for any
     * partition, the result is reported to the {@link PartitionCreationListener}, unless the
     * asynchronous creation request fails for another reason than the partition being invalid.
     *
     * @throws PartitionNotExistException if dynamic partition creation is disabled
     */
    public void createMissingPartitionAsync(
            PhysicalTablePath physicalTablePath, TableInfo tableInfo) {
        if (!dynamicPartitionEnabled) {
            throw new PartitionNotExistException(
                    String.format("Table partition '%s' does not exist.", physicalTablePath));
        }
        createPartitionOnce(physicalTablePath, tableInfo);
    }

    private void createPartitionOnce(PhysicalTablePath physicalTablePath, TableInfo tableInfo) {
        // Register the creation atomically, so at most one creation per partition is in flight.
        if (inflightPartitionsToCreate.add(physicalTablePath)) {
            LOG.info("Dynamically creating partition for {}", physicalTablePath);
            createPartition(physicalTablePath, tableInfo);
        } else {
            LOG.debug("Partition {} is already being created, skipping.", physicalTablePath);
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

    private void createPartition(PhysicalTablePath physicalTablePath, TableInfo tableInfo) {
        String partitionName = physicalTablePath.getPartitionName();
        TablePath tablePath = physicalTablePath.getTablePath();
        checkArgument(partitionName != null, "Partition name shouldn't be null.");
        ResolvedPartitionSpec resolvedPartitionSpec =
                ResolvedPartitionSpec.fromPartitionName(
                        tableInfo.getPartitionKeys(), partitionName);
        long tableId = tableInfo.getTableId();

        admin.createPartition(tablePath, resolvedPartitionSpec.toPartitionSpec(), true)
                .whenComplete(
                        (ignore, throwable) -> {
                            Throwable cause =
                                    throwable == null ? null : stripCompletionException(throwable);
                            if (cause instanceof InvalidPartitionException) {
                                onPartitionCreationCompleted(
                                        physicalTablePath,
                                        tableId,
                                        (InvalidPartitionException) cause);
                            } else if (throwable != null) {
                                // If encounter TooManyPartitionsException or
                                // TooManyBucketsException, we should set
                                // cachedCreatePartitionException to make the next createPartition
                                // call failed.
                                onPartitionCreationFailed(physicalTablePath, throwable);
                            } else {
                                onPartitionCreationSuccess(physicalTablePath, tableId);
                            }
                        });
    }

    private void onPartitionCreationSuccess(PhysicalTablePath physicalTablePath, long tableId) {
        onPartitionCreationCompleted(physicalTablePath, tableId, null);
        // TODO: trigger to update metadata here when metadataUpdater supports async update
        // metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
        LOG.info("Successfully created partition {}", physicalTablePath);
    }

    private void onPartitionCreationCompleted(
            PhysicalTablePath physicalTablePath,
            long tableId,
            @Nullable InvalidPartitionException rejection) {
        // Report the result before a later creation of the partition can complete.
        partitionCreationListener.onPartitionCreationCompleted(
                physicalTablePath, tableId, rejection);
        inflightPartitionsToCreate.remove(physicalTablePath);
    }

    private void onPartitionCreationFailed(
            PhysicalTablePath physicalTablePath, Throwable throwable) {
        inflightPartitionsToCreate.remove(physicalTablePath);
        fatalErrorHandler.accept(
                new FlussRuntimeException(
                        "Failed to dynamically create partition " + physicalTablePath,
                        stripCompletionException(throwable)));
    }

    /**
     * Receives the result of a request to create a partition, whichever write requested it, unless
     * the request failed for another reason than the partition being invalid.
     */
    @FunctionalInterface
    public interface PartitionCreationListener {

        /**
         * Called when a request to create a partition for the table with ID {@code tableId}
         * completes.
         *
         * @param rejection null if the partition was created, otherwise why the coordinator refused
         *     to create it as invalid
         */
        void onPartitionCreationCompleted(
                PhysicalTablePath physicalTablePath,
                long tableId,
                @Nullable InvalidPartitionException rejection);
    }
}
