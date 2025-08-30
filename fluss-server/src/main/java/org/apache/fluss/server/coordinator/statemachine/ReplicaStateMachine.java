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
import java.util.Optional;
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
        Set<TableBucket> allBuckets = coordinatorContext.allBuckets();
        for (TableBucket tableBucket : allBuckets) {
            List<Integer> replicas = coordinatorContext.getAssignment(tableBucket);
            for (Integer replica : replicas) {
                TableBucketReplica tableBucketReplica =
                        new TableBucketReplica(tableBucket, replica);
                if (coordinatorContext.isReplicaAndServerOnline(replica, tableBucket)) {
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
                                    LOG.error(e.getMessage());
                                    logFailedSateChange(replica, currentState, targetState);
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
                                        coordinatorContext.getBucketLeaderEpoch(
                                                replica.getTableBucket())));

                // then, may remove the offline replica from isr
                Map<TableBucketReplica, LeaderAndIsr> adjustedLeaderAndIsr =
                        doRemoveReplicaFromIsr(validReplicas);
                // notify leader and isr changes
                for (Map.Entry<TableBucketReplica, LeaderAndIsr> leaderAndIsrEntry :
                        adjustedLeaderAndIsr.entrySet()) {
                    TableBucketReplica tableBucketReplica = leaderAndIsrEntry.getKey();
                    TableBucket tableBucket = tableBucketReplica.getTableBucket();
                    LeaderAndIsr leaderAndIsr = leaderAndIsrEntry.getValue();
                    if (!coordinatorContext.isToBeDeleted(tableBucket)) {
                        Set<Integer> recipients =
                                coordinatorContext.getAssignment(tableBucket).stream()
                                        .filter(
                                                replica ->
                                                        replica != tableBucketReplica.getReplica())
                                        .collect(Collectors.toSet());
                        String partitionName;
                        try {
                            partitionName = getPartitionName(tableBucket);
                        } catch (PartitionNotExistException e) {
                            LOG.error(e.getMessage());
                            logFailedSateChange(
                                    tableBucketReplica,
                                    coordinatorContext.getReplicaState(tableBucketReplica),
                                    targetState);
                            continue;
                        }
                        // send leader request to the replica server
                        coordinatorRequestBatch.addNotifyLeaderRequestForTabletServers(
                                recipients,
                                PhysicalTablePath.of(
                                        coordinatorContext.getTablePathById(
                                                tableBucket.getTableId()),
                                        partitionName),
                                tableBucket,
                                coordinatorContext.getAssignment(tableBucket),
                                leaderAndIsr);
                    }
                }

                // finally, set to offline
                validReplicas.forEach(
                        replica -> doStateChange(replica, ReplicaState.OfflineReplica));

                break;
            case ReplicaDeletionStarted:
                validReplicas.forEach(
                        replica -> doStateChange(replica, ReplicaState.ReplicaDeletionStarted));
                // send stop replica request with delete = true
                validReplicas.forEach(
                        tableBucketReplica -> {
                            int replicaServer = tableBucketReplica.getReplica();
                            coordinatorRequestBatch.addStopReplicaRequestForTabletServers(
                                    Collections.singleton(replicaServer),
                                    tableBucketReplica.getTableBucket(),
                                    true,
                                    coordinatorContext.getBucketLeaderEpoch(
                                            tableBucketReplica.getTableBucket()));
                        });
                break;
            case ReplicaDeletionSuccessful:
                validReplicas.forEach(
                        replica -> doStateChange(replica, ReplicaState.ReplicaDeletionSuccessful));
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
                                logFailedSateChange(replica, curState, targetState);
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
            TableBucketReplica replica, ReplicaState currState, ReplicaState targetState) {
        LOG.error(
                "Fail to change state for table bucket replica {} from {} to {}.",
                stringifyReplica(replica),
                currState,
                targetState);
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

    private Map<TableBucketReplica, LeaderAndIsr> doRemoveReplicaFromIsr(
            Collection<TableBucketReplica> tableBucketReplicas) {
        Map<TableBucketReplica, LeaderAndIsr> adjustedLeaderAndIsr = new HashMap<>();
        Map<TableBucket, LeaderAndIsr> toUpdateLeaderAndIsrList = new HashMap<>();
        for (TableBucketReplica tableBucketReplica : tableBucketReplicas) {
            TableBucket tableBucket = tableBucketReplica.getTableBucket();
            int replicaId = tableBucketReplica.getReplica();

            LeaderAndIsr leaderAndIsr = null;
            if (toUpdateLeaderAndIsrList.get(tableBucket) != null) {
                leaderAndIsr = toUpdateLeaderAndIsrList.get(tableBucket);
            } else {
                Optional<LeaderAndIsr> optLeaderAndIsr =
                        coordinatorContext.getBucketLeaderAndIsr(tableBucket);
                if (!optLeaderAndIsr.isPresent()) {
                    // no leader and isr for this table bucket, skip
                    continue;
                }
                leaderAndIsr = optLeaderAndIsr.get();
            }

            if (!leaderAndIsr.isr().contains(replicaId)) {
                // isr doesn't contain the replica, skip
                continue;
            }
            int newLeader =
                    replicaId == leaderAndIsr.leader()
                            ?
                            // the leader become offline, set it to no leader
                            LeaderAndIsr.NO_LEADER
                            // otherwise, keep the origin as leader
                            : leaderAndIsr.leader();
            List<Integer> newIsr =
                    leaderAndIsr.isr().size() == 1
                            // don't remove the replica id from isr when isr size is 1,
                            // if isr is empty, we can't elect leader anymore
                            ? leaderAndIsr.isr()
                            : leaderAndIsr.isr().stream()
                                    .filter(id -> id != replicaId)
                                    .collect(Collectors.toList());
            LeaderAndIsr adjustLeaderAndIsr = leaderAndIsr.newLeaderAndIsr(newLeader, newIsr);
            adjustedLeaderAndIsr.put(tableBucketReplica, adjustLeaderAndIsr);
            toUpdateLeaderAndIsrList.put(tableBucket, adjustLeaderAndIsr);
        }
        try {
            zooKeeperClient.batchUpdateLeaderAndIsr(toUpdateLeaderAndIsrList);
            toUpdateLeaderAndIsrList.forEach(coordinatorContext::putBucketLeaderAndIsr);
            return adjustedLeaderAndIsr;
        } catch (Exception e) {
            LOG.error("Fail to batch update bucket LeaderAndIsr.", e);
        }
        return adjustedLeaderAndIsr;
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
