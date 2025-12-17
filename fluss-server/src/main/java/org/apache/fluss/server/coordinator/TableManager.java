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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.statemachine.BucketState;
import org.apache.fluss.server.coordinator.statemachine.ReplicaState;
import org.apache.fluss.server.coordinator.statemachine.ReplicaStateMachine;
import org.apache.fluss.server.coordinator.statemachine.TableBucketStateMachine;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/** A manager for tables. */
public class TableManager {
    private static final Logger LOG = LoggerFactory.getLogger(TableManager.class);

    private final MetadataManager metadataManager;
    private final RemoteStorageCleaner remoteStorageCleaner;
    private final CoordinatorContext coordinatorContext;
    private final ReplicaStateMachine replicaStateMachine;
    private final TableBucketStateMachine tableBucketStateMachine;
    private final ExecutorService ioExecutor;

    public TableManager(
            MetadataManager metadataManager,
            CoordinatorContext coordinatorContext,
            ReplicaStateMachine replicaStateMachine,
            TableBucketStateMachine tableBucketStateMachine,
            RemoteStorageCleaner remoteStorageCleaner,
            ExecutorService ioExecutor) {
        this.metadataManager = metadataManager;
        this.remoteStorageCleaner = remoteStorageCleaner;
        this.coordinatorContext = coordinatorContext;
        this.replicaStateMachine = replicaStateMachine;
        this.tableBucketStateMachine = tableBucketStateMachine;
        this.ioExecutor = ioExecutor;
    }

    public void startup() {
        LOG.info("Start up table manager.");
        replicaStateMachine.startup();
        tableBucketStateMachine.startup();
        // try to resume one deletion after start up
        resumeDeletions();
    }

    public TableBucketStateMachine getTableBucketStateMachine() {
        return tableBucketStateMachine;
    }

    public void shutdown() {
        LOG.info("Shutdown table manager.");
        tableBucketStateMachine.shutdown();
        replicaStateMachine.shutdown();
    }

    /**
     * Invoked with a created table.
     *
     * @param tablePath the table path
     * @param tableId the table id
     * @param tableAssignment the assignment for the table.
     */
    public void onCreateNewTable(
            TablePath tablePath, long tableId, TableAssignment tableAssignment) {
        coordinatorContext.putTablePath(tableId, tablePath);
        LOG.info(
                "New table: {} with id {}, new table bucket assignment {}.",
                tablePath,
                tableId,
                tableAssignment);
        for (Map.Entry<Integer, BucketAssignment> assignment :
                tableAssignment.getBucketAssignments().entrySet()) {
            int bucket = assignment.getKey();
            List<Integer> replicas = assignment.getValue().getReplicas();
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            coordinatorContext.updateBucketReplicaAssignment(tableBucket, replicas);
        }
        onCreateNewTableBucket(tableId, coordinatorContext.getAllBucketsForTable(tableId));
    }

    /**
     * Invoked with a series of created partitions for a created table.
     *
     * @param tablePath the table path
     * @param tableId the table id
     * @param partitionId the id for the created partition
     * @param partitionName the name for the created partition
     * @param partitionAssignment the assignment for the created partition.
     */
    public void onCreateNewPartition(
            TablePath tablePath,
            long tableId,
            long partitionId,
            String partitionName,
            PartitionAssignment partitionAssignment) {
        LOG.info(
                "New partition {} with partition id {} and assignment {} for table {}.",
                partitionName,
                partitionId,
                partitionAssignment,
                tablePath);
        Set<TableBucket> newTableBuckets = new HashSet<>();
        // get the partition assignment
        for (Map.Entry<Integer, BucketAssignment> assignment :
                partitionAssignment.getBucketAssignments().entrySet()) {
            int bucket = assignment.getKey();
            List<Integer> replicas = assignment.getValue().getReplicas();
            // put the bucket of the partition to context
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
            coordinatorContext.updateBucketReplicaAssignment(tableBucket, replicas);
            coordinatorContext.putPartition(
                    partitionId, PhysicalTablePath.of(tablePath, partitionName));
            newTableBuckets.add(tableBucket);
        }
        onCreateNewTableBucket(tableId, newTableBuckets);
    }

    private void onCreateNewTableBucket(long tableId, Set<TableBucket> tableBuckets) {
        LOG.info(
                "New table buckets: {} for table {}.",
                tableBuckets,
                coordinatorContext.getTablePathById(tableId));
        // first, we transmit it to state NewBucket
        tableBucketStateMachine.handleStateChange(tableBuckets, BucketState.NewBucket);
        // then get all the replicas of the all table buckets
        Set<TableBucketReplica> replicas = coordinatorContext.getBucketReplicas(tableBuckets);
        // transmit all the replicas to state NewReplica
        replicaStateMachine.handleStateChanges(replicas, ReplicaState.NewReplica);
        // transmit it to state Online
        tableBucketStateMachine.handleStateChange(tableBuckets, BucketState.OnlineBucket);
        // transmit all the replicas to state online
        replicaStateMachine.handleStateChanges(replicas, ReplicaState.OnlineReplica);
    }

    /** Invoked with a table to be deleted. */
    public void onDeleteTable(long tableId) {
        Set<TableBucket> tableBuckets = coordinatorContext.getAllBucketsForTable(tableId);
        tableBucketStateMachine.handleStateChange(tableBuckets, BucketState.OfflineBucket);
        tableBucketStateMachine.handleStateChange(tableBuckets, BucketState.NonExistentBucket);
        onDeleteTableBucket(coordinatorContext.getAllReplicasForTable(tableId));
    }

    /** Invoked with partitions of a table to be deleted. */
    public void onDeletePartition(long tableId, long partitionId) {
        Set<TableBucket> deleteBuckets =
                coordinatorContext.getAllBucketsForPartition(tableId, partitionId);
        tableBucketStateMachine.handleStateChange(deleteBuckets, BucketState.OfflineBucket);
        tableBucketStateMachine.handleStateChange(deleteBuckets, BucketState.NonExistentBucket);
        onDeleteTableBucket(coordinatorContext.getAllReplicasForPartition(tableId, partitionId));
    }

    /**
     * Invoked by {@link #onDeleteTable(long)}, {@link #onDeletePartition(long, long)} with a
     * table/partitions to be deleted,
     *
     * <p>It does the following:
     *
     * <p>1. Move all the replicas to offline state. This will send stop replica request to the
     * replicas.
     *
     * <p>2. Move all the replicas to deletion started state. This will send stop replica request
     * with delete=true which will delete all persistent data from all the replicas of the all the
     * respective buckets.
     */
    private void onDeleteTableBucket(Set<TableBucketReplica> allReplicas) {
        // to offline, send stop replica to all followers that are not in the OfflineReplica state
        // so they stop sending fetch requests to the leader
        replicaStateMachine.handleStateChanges(allReplicas, ReplicaState.OfflineReplica);
        // to deletion started
        replicaStateMachine.handleStateChanges(allReplicas, ReplicaState.ReplicaDeletionStarted);
    }

    public void resumeDeletions() {
        resumeTableDeletions();
        resumePartitionDeletions();
    }

    private void resumeTableDeletions() {
        Set<Long> tablesToBeDeleted = new HashSet<>(coordinatorContext.getTablesToBeDeleted());
        Set<Long> eligibleTableDeletion = new HashSet<>();

        for (long tableId : tablesToBeDeleted) {
            // if all replicas are marked as deleted successfully, then table deletion is done
            if (coordinatorContext.areAllReplicasInState(
                    tableId, ReplicaState.ReplicaDeletionSuccessful)) {
                completeDeleteTable(tableId);
                LOG.info("Deletion of table with id {} successfully completed.", tableId);
            }
            if (isEligibleForDeletion(tableId)) {
                eligibleTableDeletion.add(tableId);
            }
        }
        if (!eligibleTableDeletion.isEmpty()) {
            for (long tableId : eligibleTableDeletion) {
                onDeleteTable(tableId);
            }
        }
    }

    private void resumePartitionDeletions() {
        Set<TablePartition> partitionsToDelete =
                new HashSet<>(coordinatorContext.getPartitionsToBeDeleted());
        Set<TablePartition> eligiblePartitionDeletion = new HashSet<>();

        for (TablePartition partition : partitionsToDelete) {
            // if all replicas are marked as deleted successfully, then partition deletion is done
            if (coordinatorContext.areAllReplicasInState(
                    partition, ReplicaState.ReplicaDeletionSuccessful)) {
                completeDeletePartition(partition);
                LOG.info("Deletion of partition {} successfully completed.", partition);
            }
            if (isEligibleForDeletion(partition)) {
                eligiblePartitionDeletion.add(partition);
            }
        }
        if (!eligiblePartitionDeletion.isEmpty()) {
            for (TablePartition partition : eligiblePartitionDeletion) {
                onDeletePartition(partition.getTableId(), partition.getPartitionId());
            }
        }
    }

    private void completeDeleteTable(long tableId) {
        Set<TableBucketReplica> replicas = coordinatorContext.getAllReplicasForTable(tableId);
        replicaStateMachine.handleStateChanges(replicas, ReplicaState.NonExistentReplica);
        asyncDeleteRemoteDirectory(tableId);
        asyncDeleteTableMetadata(tableId);
        coordinatorContext.removeTable(tableId);
    }

    private void completeDeletePartition(TablePartition tablePartition) {
        Set<TableBucketReplica> replicas =
                coordinatorContext.getAllReplicasForPartition(
                        tablePartition.getTableId(), tablePartition.getPartitionId());
        replicaStateMachine.handleStateChanges(replicas, ReplicaState.NonExistentReplica);
        asyncDeleteRemoteDirectory(tablePartition);
        asyncDeletePartitionMetadata(tablePartition.getPartitionId());
        coordinatorContext.removePartition(tablePartition);
    }

    private void asyncDeleteRemoteDirectory(long tableId) {
        // delete table remote dir, when restore the coordinator, the table info will be null
        // we can't delete the remote dir since we don't know tablePath now
        TableInfo tableInfo = coordinatorContext.getTableInfoById(tableId);
        if (tableInfo != null) {
            remoteStorageCleaner.asyncDeleteTableRemoteDir(
                    tableInfo.getTablePath(), tableInfo.hasPrimaryKey(), tableId);
        }
    }

    private void asyncDeleteRemoteDirectory(TablePartition tablePartition) {
        // delete partition remote dir, when restore the coordinator, the table info will be null
        // we can't delete the remote dir since we don't tablePath and partition name now
        TableInfo tableInfo = coordinatorContext.getTableInfoById(tablePartition.getTableId());
        if (tableInfo != null) {
            String partitionName =
                    coordinatorContext.getPartitionName(tablePartition.getPartitionId());
            if (partitionName != null) {
                remoteStorageCleaner.asyncDeletePartitionRemoteDir(
                        PhysicalTablePath.of(tableInfo.getTablePath(), partitionName),
                        tableInfo.hasPrimaryKey(),
                        tablePartition);
            }
        }
    }

    private void asyncDeleteTableMetadata(long tableId) {
        ioExecutor.submit(
                () -> {
                    try {
                        metadataManager.completeDeleteTable(tableId);
                    } catch (Exception e) {
                        LOG.error("Fail to delete ZooKeeper metadata for table id {}.", tableId, e);
                    }
                });
    }

    private void asyncDeletePartitionMetadata(long partitionId) {
        ioExecutor.submit(
                () -> {
                    try {
                        metadataManager.completeDeletePartition(partitionId);
                    } catch (Exception e) {
                        LOG.error(
                                "Fail to delete ZooKeeper metadata for partition id {}.",
                                partitionId,
                                e);
                    }
                });
    }

    private boolean isEligibleForDeletion(long tableId) {
        // the table is queued for deletion and
        // no any replica is in state deletion started
        return coordinatorContext.isTableQueuedForDeletion(tableId)
                && !coordinatorContext.isAnyReplicaInState(
                        tableId, ReplicaState.ReplicaDeletionStarted);
    }

    private boolean isEligibleForDeletion(TablePartition tablePartition) {
        // the partition is queued for deletion and
        // no any replica is in state deletion started
        return coordinatorContext.isPartitionQueuedForDeletion(tablePartition)
                && !coordinatorContext.isAnyReplicaInState(
                        tablePartition, ReplicaState.ReplicaDeletionStarted);
    }
}
