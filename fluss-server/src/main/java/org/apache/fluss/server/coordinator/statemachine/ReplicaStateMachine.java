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

package org.apache.fluss.server.coordinator.statemachine;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.CoordinatorRequestBatch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A state machine for the replica of table bucket. */
public class ReplicaStateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaStateMachine.class);

    private final CoordinatorContext coordinatorContext;

    private final CoordinatorRequestBatch coordinatorRequestBatch;
    private final ZooKeeperClient zooKeeperClient;

    public ReplicaStateMachine(
            CoordinatorContext coordinatorContext,
            CoordinatorRequestBatch coordinatorRequestBatch,
            ZooKeeperClient zooKeeperClient) {
        this.coordinatorContext = coordinatorContext;
        this.coordinatorRequestBatch = coordinatorRequestBatch;
        this.zooKeeperClient = zooKeeperClient;
    }

    public void startup() {
        LOG.info("Initializing replica state machine.");
        Tuple2<Set<TableBucketReplica>, Set<TableBucketReplica>> onlineAndOfflineReplicas =
                initializeReplicaState();

        // we only try to trigger the replicas that are not in deleted table to online
        // since it will then trigger the replicas to be offline/deleted during deletion resuming
        // see more in TableManager#onDeleteTableBucket
        LOG.info("Triggering online replica state changes");
        handleStateChanges(
                replicaNotInDeletedTableOrPartition(onlineAndOfflineReplicas.f0),
                ReplicaState.OnlineReplica);
        LOG.info("Triggering offline replica state changes");
        handleStateChanges(
                replicaNotInDeletedTableOrPartition(onlineAndOfflineReplicas.f1),
                ReplicaState.OfflineReplica);
        LOG.debug(
                "Started replica state machine with initial state {}.",
                coordinatorContext.getReplicaStates());
    }

    private Set<TableBucketReplica> replicaNotInDeletedTableOrPartition(
            Set<TableBucketReplica> replicas) {
        return replicas.stream()
                .filter(replica -> !coordinatorContext.isToBeDeleted(replica.getTableBucket()))
                .collect(Collectors.toSet());
    }

    public void shutdown() {
        LOG.info("Shutdown replica state machine.");
    }

    private Tuple2<Set<TableBucketReplica>, Set<TableBucketReplica>> initializeReplicaState() {
        Set<TableBucketReplica> onlineReplicas = new HashSet<>();
        Set<TableBucketReplica> offlineReplicas = new HashSet<>();
        Set<TableBucket> allBuckets = coordinatorContext.getAllBuckets();
        for (TableBucket tableBucket : allBuckets) {
            List<Integer> replicas = coordinatorContext.getAssignment(tableBucket);
            for (Integer replica : replicas) {
                TableBucketReplica tableBucketReplica =
                        new TableBucketReplica(tableBucket, replica);
                if (coordinatorContext.isReplicaOnline(replica, tableBucket)) {
                    coordinatorContext.putReplicaState(
                            tableBucketReplica, ReplicaState.OnlineReplica);
                    onlineReplicas.add(tableBucketReplica);
                } else {
                    coordinatorContext.putReplicaState(
                            tableBucketReplica, ReplicaState.OfflineReplica);
                    offlineReplicas.add(tableBucketReplica);
                }
            }
        }
        return Tuple2.of(onlineReplicas, offlineReplicas);
    }

    public void handleStateChanges(
            Collection<TableBucketReplica> replicas, ReplicaState targetState) {
        try {
            coordinatorRequestBatch.newBatch();
            doHandleStateChanges(replicas, targetState);
            coordinatorRequestBatch.sendRequestToTabletServers(
                    coordinatorContext.getCoordinatorEpoch());
        } catch (Throwable e) {
            LOG.error(
                    "Failed to move table bucket replicas {} to state {}.",
                    replicas,
                    targetState,
                    e);
        }
    }

    /**
     * Handle the state change of table bucket replica. It's the core state transition logic of the
     * state machine. It ensures that every state transition happens from a legal previous state to
     * the target state. The valid state transitions for the state machine are as follows:
     *
     * <p>NonExistentReplica -> NewReplica:
     *
     * <p>-- Case1: Create a new replica for table bucket whiling creating a new table. Do: mark it
     * as NewReplica
     *
     * <p>-- Case2: Table reassignment. Do: send leader request to tablet server which will trigger
     * create a new replica, and mark it as NewReplica
     *
     * <p>NewReplica -> OnlineReplica:
     *
     * <p>-- Case1: The following steps for creating a new table after mark it as NewReplica. Do:
     * mark it as OnlineReplica
     *
     * <p>-- Case2: Table reassignment. Do: add the new replicas to the table bucket assignment in
     * the state machine, and mark it as OnlineReplica
     *
     * <p>OnlineReplica -> OnlineReplica:
     *
     * <p>-- When Coordinator server startup, it'll mark all the replica as online directly. Do:
     * send leader request with isNew = false to the tablet server so that it can know it's really
     * online or not, and mark it as OnlineReplica
     *
     * <p>OfflineReplica -> OnlineReplica:
     *
     * <p>--The tablet server fail over, the replicas it hold will first transmit to offline but
     * then to online. Do: send leader request with isNew = false to the tablet server so that it
     * can know it's really online or not, and mark it as OnlineReplica
     *
     * <p>NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible -> OfflineReplica
     *
     * <p>-- For OnlineReplica -> OfflineReplica, it happens the server fail; Do: send leader
     * request to the servers that hold the other replicas the replica is offline
     *
     * <p>-- NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible -> OfflineReplica,
     * it happens that the table is dropped. Do: send stop replica request with delete is false to
     * the servers, and mark it as OfflineReplica
     *
     * <p>OfflineReplica -> ReplicaDeletionStarted
     *
     * <p>-- It happens that the table is dropped. Do: send stop replica request with delete is true
     * to the servers, and mark it as ReplicaDeletionStarted
     *
     * <p>ReplicaDeletionStarted -> ReplicaDeletionSuccessful
     *
     * <p>-- It happens that the table is dropped and receive successful response for stopping
     * replica with delete is true from tablet server. Do, mark it as ReplicaDeletionSuccessful, and
     * try to resume table deletion which will really delete the table when all the replicas are in
     * ReplicaDeletionSuccessful
     *
     * <p>OfflineReplica, ReplicaDeletionStarted -> ReplicaDeletionIneligible
     *
     * <p>-- It happens when a tablet server holding a replica to be deleted is offline. Mark the
     * replica as ReplicaDeletionIneligible until the tablet server comes back.
     *
     * <p>ReplicaDeletionIneligible -> OfflineReplica -> ReplicaDeletionStarted
     *
     * <p>-- It happens when the tablet server comes back. Move the replica through OfflineReplica
     * and retry the StopReplica request with delete is true.
     *
     * <p>ReplicaDeletionSuccessful -> NonExistentReplica
     *
     * <p>-- It happens that the table is deleted successfully. Remove the replicas from state
     * machine.
     *
     * @param replicas The table bucket replicas that are to do state change
     * @param targetState the target state that is to change to
     */
    private void doHandleStateChanges(
            Collection<TableBucketReplica> replicas, ReplicaState targetState) {
        replicas.forEach(
                replica ->
                        coordinatorContext.putReplicaStateIfNotExists(
                                replica, ReplicaState.NonExistentReplica));
        Collection<TableBucketReplica> validReplicas =
                checkValidReplicaStateChange(replicas, targetState);
        switch (targetState) {
            case NewReplica:
                validReplicas.forEach(replica -> doStateChange(replica, targetState));
                break;
            case OnlineReplica:
                validReplicas.forEach(
                        replica -> {
                            ReplicaState currentState = coordinatorContext.getReplicaState(replica);
                            if (currentState != ReplicaState.NewReplica) {
                                TableBucket tableBucket = replica.getTableBucket();
                                String partitionName;
                                try {
                                    partitionName = getPartitionName(tableBucket);
                                } catch (PartitionNotExistException e) {
                                    logFailedSateChange(
                                            replica, currentState, targetState, e.getMessage());
                                    return;
                                }

                                coordinatorContext
                                        .getBucketLeaderAndIsr(tableBucket)
                                        .ifPresent(
                                                leaderAndIsr -> {
                                                    // send leader request to the replica server
                                                    coordinatorRequestBatch
                                                            .addNotifyLeaderRequestForTabletServers(
                                                                    Collections.singleton(
                                                                            replica.getReplica()),
                                                                    PhysicalTablePath.of(
                                                                            coordinatorContext
                                                                                    .getTablePathById(
                                                                                            tableBucket
                                                                                                    .getTableId()),
                                                                            partitionName),
                                                                    replica.getTableBucket(),
                                                                    coordinatorContext
                                                                            .getAssignment(
                                                                                    tableBucket),
                                                                    leaderAndIsr);
                                                });
                            }
                            doStateChange(replica, targetState);
                        });
                break;
            case OfflineReplica:
                // first, send stop replica request to servers
                validReplicas.forEach(
                        replica ->
                                coordinatorRequestBatch.addStopReplicaRequestForTabletServers(
                                        Collections.singleton(replica.getReplica()),
                                        replica.getTableBucket(),
                                        false,
                                        false,
                                        coordinatorContext.getBucketLeaderEpoch(
                                                replica.getTableBucket())));

                doRemoveReplicaFromIsr(validReplicas).forEach(this::notifyReplicasAfterIsrChange);

                // finally, set to offline
                validReplicas.forEach(
                        replica -> doStateChange(replica, ReplicaState.OfflineReplica));

                break;
            case ReplicaMigrationStarted:
                validReplicas.forEach(
                        replica -> doStateChange(replica, ReplicaState.ReplicaMigrationStarted));
                validReplicas.forEach(
                        tableBucketReplica -> {
                            int replicaServer = tableBucketReplica.getReplica();
                            // send stop replica request with deleteLocal = true and deleteRemote =
                            // false indicates the replica is migrated.
                            coordinatorRequestBatch.addStopReplicaRequestForTabletServers(
                                    Collections.singleton(replicaServer),
                                    tableBucketReplica.getTableBucket(),
                                    true,
                                    false,
                                    coordinatorContext.getBucketLeaderEpoch(
                                            tableBucketReplica.getTableBucket()));
                        });
                break;
            case ReplicaDeletionStarted:
                validReplicas.forEach(
                        replica -> doStateChange(replica, ReplicaState.ReplicaDeletionStarted));
                // send stop replica request with deleteLocal = true and delete = true indicates the
                // replica is deleted.
                validReplicas.forEach(
                        tableBucketReplica -> {
                            int replicaServer = tableBucketReplica.getReplica();
                            coordinatorRequestBatch.addStopReplicaRequestForTabletServers(
                                    Collections.singleton(replicaServer),
                                    tableBucketReplica.getTableBucket(),
                                    true,
                                    true,
                                    coordinatorContext.getBucketLeaderEpoch(
                                            tableBucketReplica.getTableBucket()));
                        });
                break;
            case ReplicaDeletionSuccessful:
                validReplicas.forEach(
                        replica -> doStateChange(replica, ReplicaState.ReplicaDeletionSuccessful));
                break;
            case ReplicaDeletionIneligible:
                validReplicas.forEach(
                        replica -> doStateChange(replica, ReplicaState.ReplicaDeletionIneligible));
                break;
            case NonExistentReplica:
                validReplicas.forEach(replica -> doStateChange(replica, null));
                break;
        }
    }

    @VisibleForTesting
    protected Collection<TableBucketReplica> checkValidReplicaStateChange(
            Collection<TableBucketReplica> replicas, ReplicaState targetState) {
        return replicas.stream()
                .filter(
                        replica -> {
                            ReplicaState curState = coordinatorContext.getReplicaState(replica);
                            if (isValidReplicaStateTransition(curState, targetState)) {
                                return true;
                            } else {
                                logInvalidTransition(replica, curState, targetState);
                                logFailedSateChange(
                                        replica,
                                        curState,
                                        targetState,
                                        "Invalid Replica State Transition.");
                                return false;
                            }
                        })
                .collect(Collectors.toList());
    }

    private boolean isValidReplicaStateTransition(
            ReplicaState currentState, ReplicaState targetState) {
        return targetState.getValidPreviousStates().contains(currentState);
    }

    private void doStateChange(TableBucketReplica replica, @Nullable ReplicaState targetState) {
        ReplicaState previousState;
        if (targetState != null) {
            previousState = coordinatorContext.putReplicaState(replica, targetState);
        } else {
            previousState = coordinatorContext.removeReplicaState(replica);
        }
        logSuccessfulStateChange(replica, previousState, targetState);
    }

    private void logInvalidTransition(
            TableBucketReplica replica, ReplicaState curState, ReplicaState targetState) {
        LOG.error(
                "Replica state for {} should be in the {} before moving to state {}, but the current state is {}.",
                stringifyReplica(replica),
                targetState.getValidPreviousStates(),
                targetState,
                curState);
    }

    private void logFailedSateChange(
            TableBucketReplica replica,
            ReplicaState currState,
            ReplicaState targetState,
            String reason) {
        LOG.error(
                "Fail to change state for table bucket replica {} from {} to {}, reason: {}.",
                stringifyReplica(replica),
                currState,
                targetState,
                reason);
    }

    private void logSuccessfulStateChange(
            TableBucketReplica replica, ReplicaState currState, ReplicaState targetState) {
        LOG.debug(
                "Successfully changed state for table bucket replica {} from {} to {}.",
                stringifyReplica(replica),
                currState,
                targetState);
    }

    private String stringifyReplica(TableBucketReplica replica) {
        TableBucket tableBucket = replica.getTableBucket();
        if (tableBucket.getPartitionId() == null) {
            return String.format(
                    "TableBucketReplica{tableBucket=%s, replica=%d, tablePath=%s}",
                    tableBucket,
                    replica.getReplica(),
                    coordinatorContext.getTablePathById(tableBucket.getTableId()));
        } else {
            return String.format(
                    "TableBucketReplica{tableBucket=%s, replica=%d, tablePath=%s, partition=%s}",
                    tableBucket,
                    replica.getReplica(),
                    coordinatorContext.getTablePathById(replica.getTableBucket().getTableId()),
                    coordinatorContext.getPartitionName(tableBucket.getPartitionId()));
        }
    }

    private void notifyReplicasAfterIsrChange(
            TableBucket bucket, Set<TableBucketReplica> offlineReplicas) {
        if (coordinatorContext.isToBeDeleted(bucket)) {
            return;
        }
        String partitionName;
        try {
            partitionName = getPartitionName(bucket);
        } catch (PartitionNotExistException e) {
            for (TableBucketReplica replica : offlineReplicas) {
                logFailedSateChange(
                        replica,
                        coordinatorContext.getReplicaState(replica),
                        ReplicaState.OfflineReplica,
                        e.getMessage());
            }
            return;
        }

        List<Integer> assignment = coordinatorContext.getAssignment(bucket);
        Set<Integer> recipients = new HashSet<>(assignment);
        // Each offline replica notifies its peers. With multiple removals their peers cover the
        // assignment.
        if (offlineReplicas.size() == 1) {
            recipients.remove(offlineReplicas.iterator().next().getReplica());
        }
        coordinatorRequestBatch.addNotifyLeaderRequestForTabletServers(
                recipients,
                PhysicalTablePath.of(
                        coordinatorContext.getTablePathById(bucket.getTableId()), partitionName),
                bucket,
                assignment,
                coordinatorContext.getBucketLeaderAndIsr(bucket).get());
    }

    private Map<TableBucket, Set<TableBucketReplica>> doRemoveReplicaFromIsr(
            Collection<TableBucketReplica> tableBucketReplicas) {
        Map<TableBucket, Set<TableBucketReplica>> removed = new HashMap<>();
        Map<TableBucket, LeaderAndIsr> currentRoles = coordinatorContext.bucketLeaderAndIsr();
        try {
            for (int attempt = 0; attempt < 2; attempt++) {
                Map<TableBucket, LeaderAndIsr> changes = new HashMap<>();
                Map<TableBucket, Set<TableBucketReplica>> affected = new HashMap<>();
                for (TableBucketReplica replica : tableBucketReplicas) {
                    TableBucket bucket = replica.getTableBucket();
                    LeaderAndIsr role = changes.get(bucket);
                    if (role == null) {
                        role = currentRoles.get(bucket);
                    }
                    if (role != null && role.isr().contains(replica.getReplica())) {
                        changes.put(bucket, removeReplicaFromIsr(role, replica.getReplica()));
                        affected.computeIfAbsent(bucket, ignored -> new HashSet<>()).add(replica);
                    }
                }
                Map<TableBucket, LeaderAndIsr> committed =
                        zooKeeperClient.batchUpdateLeaderAndIsr(
                                changes, coordinatorContext.getCoordinatorZkVersion());
                committed.forEach(
                        (bucket, role) -> {
                            coordinatorContext.putBucketLeaderAndIsr(bucket, role);
                            removed.put(bucket, affected.get(bucket));
                        });
                if (attempt > 0 || changes.size() == committed.size()) {
                    break;
                }
                // An import can advance the epoch without removing these offline replicas.
                changes.keySet().removeAll(committed.keySet());
                currentRoles = zooKeeperClient.getLeaderAndIsrs(changes.keySet());
                currentRoles.forEach(coordinatorContext::putBucketLeaderAndIsr);
            }
        } catch (Exception e) {
            LOG.error("Fail to batch update bucket LeaderAndIsr.", e);
        }
        return removed;
    }

    private static LeaderAndIsr removeReplicaFromIsr(LeaderAndIsr role, int replicaId) {
        // Keep the last ISR member eligible for a future election.
        List<Integer> isr =
                role.isr().size() == 1
                        ? role.isr()
                        : role.isr().stream()
                                .filter(id -> id != replicaId)
                                .collect(Collectors.toList());
        return replicaId == role.leader() || role.leader() == LeaderAndIsr.NO_LEADER
                ? role.newLeaderAndIsr(LeaderAndIsr.NO_LEADER, isr, role.standbyReplicas())
                : role.newLeaderAndIsr(isr);
    }

    @Nullable
    private String getPartitionName(TableBucket tableBucket) throws PartitionNotExistException {
        String partitionName;
        if (tableBucket.getPartitionId() != null) {
            partitionName = coordinatorContext.getPartitionName(tableBucket.getPartitionId());
            if (partitionName == null) {
                throw new PartitionNotExistException(
                        String.format(
                                "Can't find partition name for partition id: %s.",
                                tableBucket.getPartitionId()));
            }
        } else {
            partitionName = null;
        }
        return partitionName;
    }
}
