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

package org.apache.fluss.server.coordinator.rebalance;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.exception.RebalanceFailureException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.CoordinatorEventProcessor;
import org.apache.fluss.server.coordinator.rebalance.goal.Goal;
import org.apache.fluss.server.coordinator.rebalance.goal.GoalOptimizer;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.RackModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RebalancePlan;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.CANCELED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FAILED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * A rebalance manager to generate rebalance plan, and execution rebalance plan.
 *
 * <p>This manager can only be used in {@link CoordinatorEventProcessor} as a single threaded model.
 */
@ThreadSafe
public class RebalanceManager {

    private static final Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);
    private static final Set<RebalanceStatus> FINAL_STATUS =
            new HashSet<>(Arrays.asList(COMPLETED, CANCELED, FAILED));

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Lock lock = new ReentrantLock();
    private final ZooKeeperClient zkClient;
    private final CoordinatorEventProcessor eventProcessor;

    @GuardedBy("lock")
    private final Queue<TableBucket> ongoingRebalanceTasksQueue = new ArrayDeque<>();

    /** A mapping from table bucket to rebalance status of pending and running tasks. */
    @GuardedBy("lock")
    private final Map<TableBucket, RebalanceResultForBucket> ongoingRebalanceTasks =
            MapUtils.newConcurrentHashMap();

    /** A mapping from table bucket to rebalance status of failed or completed tasks. */
    @GuardedBy("lock")
    private final Map<TableBucket, RebalanceResultForBucket> finishedRebalanceTasks =
            MapUtils.newConcurrentHashMap();

    @GuardedBy("lock")
    private final GoalOptimizer goalOptimizer;

    @GuardedBy("lock")
    private long registerTime;

    public RebalanceManager(CoordinatorEventProcessor eventProcessor, ZooKeeperClient zkClient) {
        this.eventProcessor = eventProcessor;
        this.zkClient = zkClient;
        this.goalOptimizer = new GoalOptimizer();
    }

    public void startup() {
        LOG.info("Start up rebalance manager.");
        initialize();
    }

    private void initialize() {
        try {
            zkClient.getRebalancePlan()
                    .ifPresent(rebalancePlan -> registerRebalance(rebalancePlan.getExecutePlan()));
        } catch (Exception e) {
            LOG.error(
                    "Failed to get rebalance plan from zookeeper, it will be treated as no"
                            + "rebalance tasks.",
                    e);
        }
    }

    public void registerRebalance(Map<TableBucket, RebalancePlanForBucket> rebalancePlan) {
        checkNotClosed();
        registerTime = System.currentTimeMillis();
        // Register to zookeeper first.
        try {
            Optional<RebalancePlan> existPlanOpt = zkClient.getRebalancePlan();
            if (!existPlanOpt.isPresent()) {
                zkClient.registerRebalancePlan(new RebalancePlan(NOT_STARTED, rebalancePlan));
            } else {
                RebalancePlan existPlan = existPlanOpt.get();
                if (FINAL_STATUS.contains(existPlan.getRebalanceStatus())) {
                    zkClient.updateRebalancePlan(
                            new RebalancePlan(NOT_STARTED, existPlanOpt.get().getExecutePlan()));
                } else {
                    throw new RebalanceFailureException(
                            "Rebalance task already exists. Please wait for it to finish or cancel it first.");
                }
            }
        } catch (Exception e) {
            LOG.error("Error when register rebalance plan to zookeeper.", e);
            throw new RebalanceFailureException(
                    "Error when register rebalance plan to zookeeper.", e);
        }

        inLock(
                lock,
                () -> {
                    // Then, register to ongoingRebalanceTasks.
                    rebalancePlan.forEach(
                            ((tableBucket, rebalancePlanForBucket) -> {
                                ongoingRebalanceTasksQueue.add(tableBucket);
                                ongoingRebalanceTasks.put(
                                        tableBucket,
                                        RebalanceResultForBucket.of(
                                                rebalancePlanForBucket, NOT_STARTED));
                            }));

                    // Trigger one rebalance task to execute.
                    processNewRebalanceTask();
                });
    }

    public void finishRebalanceTask(TableBucket tableBucket, RebalanceStatus statusForBucket) {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    if (ongoingRebalanceTasksQueue.contains(tableBucket)) {
                        ongoingRebalanceTasksQueue.remove(tableBucket);
                        RebalanceResultForBucket resultForBucket =
                                ongoingRebalanceTasks.remove(tableBucket);
                        checkNotNull(resultForBucket, "RebalanceResultForBucket is null.");
                        finishedRebalanceTasks.put(
                                tableBucket,
                                RebalanceResultForBucket.of(
                                        resultForBucket.plan(), statusForBucket));
                        LOG.info(
                                "Rebalance in progress: {} tasks pending, {} completed.",
                                ongoingRebalanceTasksQueue.size(),
                                finishedRebalanceTasks.size());

                        if (ongoingRebalanceTasksQueue.isEmpty()) {
                            // All rebalance tasks are completed.
                            completeRebalance();
                        } else {
                            // Trigger one rebalance task to execute.
                            processNewRebalanceTask();
                        }
                    }
                });
    }

    public void cancelRebalance() {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    try {
                        Optional<RebalancePlan> rebalancePlanOpt = zkClient.getRebalancePlan();
                        if (rebalancePlanOpt.isPresent()) {
                            zkClient.updateRebalancePlan(
                                    new RebalancePlan(
                                            CANCELED, rebalancePlanOpt.get().getExecutePlan()));
                        }
                    } catch (Exception e) {
                        LOG.error("Error when delete rebalance plan from zookeeper.", e);
                    }

                    ongoingRebalanceTasksQueue.clear();
                    ongoingRebalanceTasks.clear();
                    finishedRebalanceTasks.clear();
                    LOG.info("Cancel rebalance task success.");
                });
    }

    public boolean hasOngoingRebalance() {
        checkNotClosed();
        return inLock(
                lock, () -> !ongoingRebalanceTasks.isEmpty() || !finishedRebalanceTasks.isEmpty());
    }

    public RebalancePlan generateRebalancePlan(List<Goal> goalsByPriority) {
        checkNotClosed();
        List<RebalancePlanForBucket> rebalancePlanForBuckets;
        try {
            // Generate the latest cluster model.
            ClusterModel clusterModel = buildClusterModel(eventProcessor.getCoordinatorContext());

            // do optimize.
            rebalancePlanForBuckets = goalOptimizer.doOptimizeOnce(clusterModel, goalsByPriority);
        } catch (Exception e) {
            LOG.error("Failed to generate rebalance plan.", e);
            throw e;
        }

        // group by tableId and partitionId to generate rebalance plan.
        return buildRebalancePlan(rebalancePlanForBuckets);
    }

    public @Nullable RebalancePlanForBucket getRebalancePlanForBucket(TableBucket tableBucket) {
        checkNotClosed();
        return inLock(
                lock,
                () -> {
                    RebalanceResultForBucket resultForBucket =
                            ongoingRebalanceTasks.get(tableBucket);
                    if (resultForBucket != null) {
                        return resultForBucket.plan();
                    }
                    return null;
                });
    }

    private void processNewRebalanceTask() {
        TableBucket tableBucket = ongoingRebalanceTasksQueue.peek();
        if (tableBucket != null && ongoingRebalanceTasks.containsKey(tableBucket)) {
            RebalanceResultForBucket resultForBucket = ongoingRebalanceTasks.get(tableBucket);
            RebalanceResultForBucket rebalanceResultForBucket =
                    RebalanceResultForBucket.of(resultForBucket.plan(), REBALANCING);
            eventProcessor.tryToExecuteRebalanceTask(rebalanceResultForBucket.plan());
        }
    }

    private void completeRebalance() {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    try {
                        Optional<RebalancePlan> rebalancePlanOpt = zkClient.getRebalancePlan();
                        if (rebalancePlanOpt.isPresent()) {
                            zkClient.updateRebalancePlan(
                                    new RebalancePlan(
                                            COMPLETED, rebalancePlanOpt.get().getExecutePlan()));
                        }
                    } catch (Exception e) {
                        LOG.error("Error when update rebalance plan from zookeeper.", e);
                    }

                    ongoingRebalanceTasks.clear();
                    finishedRebalanceTasks.clear();
                    LOG.info(
                            "Rebalance complete with {} ms.",
                            System.currentTimeMillis() - registerTime);
                });
    }

    private ClusterModel buildClusterModel(CoordinatorContext coordinatorContext) {
        Map<Integer, ServerInfo> liveTabletServers = coordinatorContext.getLiveTabletServers();
        Map<Integer, ServerTag> serverTags = coordinatorContext.getServerTags();

        Map<Integer, ServerModel> serverModelMap = new HashMap<>();
        for (ServerInfo serverInfo : liveTabletServers.values()) {
            Integer id = serverInfo.id();
            String rack = serverInfo.rack() == null ? RackModel.DEFAULT_RACK : serverInfo.rack();
            if (serverTags.containsKey(id)) {
                serverModelMap.put(
                        id, new ServerModel(id, rack, !isServerOffline(serverTags.get(id))));
            } else {
                serverModelMap.put(id, new ServerModel(id, rack, true));
            }
        }

        ClusterModel clusterModel = initialClusterModel(serverModelMap);

        // Try to update the cluster model with the latest bucket states.
        Set<TableBucket> allBuckets = coordinatorContext.getAllBuckets();
        for (TableBucket tableBucket : allBuckets) {
            List<Integer> assignment = coordinatorContext.getAssignment(tableBucket);
            Optional<LeaderAndIsr> bucketLeaderAndIsrOpt =
                    coordinatorContext.getBucketLeaderAndIsr(tableBucket);
            checkArgument(bucketLeaderAndIsrOpt.isPresent(), "Bucket leader and isr is empty.");
            LeaderAndIsr isr = bucketLeaderAndIsrOpt.get();
            int leader = isr.leader();
            for (int i = 0; i < assignment.size(); i++) {
                int replica = assignment.get(i);
                clusterModel.createReplica(replica, tableBucket, i, leader == replica);
            }
        }
        return clusterModel;
    }

    private RebalancePlan buildRebalancePlan(List<RebalancePlanForBucket> rebalancePlanForBuckets) {
        Map<TableBucket, RebalancePlanForBucket> bucketPlan = new HashMap<>();
        for (RebalancePlanForBucket rebalancePlanForBucket : rebalancePlanForBuckets) {
            bucketPlan.put(rebalancePlanForBucket.getTableBucket(), rebalancePlanForBucket);
        }
        return new RebalancePlan(NOT_STARTED, bucketPlan);
    }

    private boolean isServerOffline(ServerTag serverTag) {
        return serverTag == ServerTag.PERMANENT_OFFLINE || serverTag == ServerTag.TEMPORARY_OFFLINE;
    }

    private ClusterModel initialClusterModel(Map<Integer, ServerModel> serverModelMap) {
        SortedSet<ServerModel> servers = new TreeSet<>(serverModelMap.values());
        return new ClusterModel(servers);
    }

    private void checkNotClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("RebalanceManager is already closed.");
        }
    }

    public void close() {
        isClosed.compareAndSet(false, true);
    }

    @VisibleForTesting
    public ClusterModel buildClusterModel() {
        return buildClusterModel(eventProcessor.getCoordinatorContext());
    }
}
