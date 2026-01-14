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
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.exception.NoRebalanceInProgressException;
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
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.CANCELED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FINAL_STATUSES;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A rebalance manager to generate rebalance plan, and execution rebalance plan.
 *
 * <p>This manager can only be used in {@link CoordinatorEventProcessor} as a single threaded model.
 */
public class RebalanceManager {
    private static final Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    private final ZooKeeperClient zkClient;
    private final CoordinatorEventProcessor eventProcessor;

    /** A queue of in progress table bucket to rebalance. */
    private final Queue<TableBucket> inProgressRebalanceTasksQueue = new ArrayDeque<>();

    /** A mapping from table bucket to rebalance status of pending and running tasks. */
    private final Map<TableBucket, RebalanceResultForBucket> inProgressRebalanceTasks =
            MapUtils.newConcurrentHashMap();

    /** A mapping from table bucket to rebalance status of failed or completed tasks. */
    private final Map<TableBucket, RebalanceResultForBucket> finishedRebalanceTasks =
            MapUtils.newConcurrentHashMap();

    private final GoalOptimizer goalOptimizer;
    private volatile long registerTime;
    private volatile @Nullable RebalanceStatus rebalanceStatus;
    private volatile @Nullable String currentRebalanceId;
    private volatile boolean isClosed = false;

    public RebalanceManager(CoordinatorEventProcessor eventProcessor, ZooKeeperClient zkClient) {
        this.eventProcessor = eventProcessor;
        this.zkClient = zkClient;
        this.goalOptimizer = new GoalOptimizer();
    }

    public void startup() {
        LOG.info("Start up rebalance manager.");
        initialize();
    }

    public @Nullable String getRebalanceId() {
        return currentRebalanceId;
    }

    private void initialize() {
        try {
            zkClient.getRebalanceTask()
                    .ifPresent(
                            rebalancePlan ->
                                    registerRebalance(
                                            rebalancePlan.getRebalanceId(),
                                            rebalancePlan.getExecutePlan(),
                                            rebalancePlan.getRebalanceStatus()));
        } catch (Exception e) {
            LOG.error(
                    "Failed to get rebalance plan from zookeeper, it will be treated as no"
                            + "rebalance tasks.",
                    e);
        }
    }

    public void registerRebalance(
            String rebalanceId,
            Map<TableBucket, RebalancePlanForBucket> rebalancePlan,
            RebalanceStatus newStatus) {
        checkNotClosed();
        registerTime = System.currentTimeMillis();
        // first clear all exists tasks.
        inProgressRebalanceTasks.clear();
        inProgressRebalanceTasksQueue.clear();
        finishedRebalanceTasks.clear();

        currentRebalanceId = rebalanceId;
        if (rebalancePlan.isEmpty()) {
            completeRebalance();
            return;
        }

        rebalancePlan.forEach(
                ((tableBucket, planForBucket) -> {
                    if (FINAL_STATUSES.contains(newStatus)) {
                        finishedRebalanceTasks.put(
                                tableBucket, RebalanceResultForBucket.of(planForBucket, newStatus));
                    } else {
                        inProgressRebalanceTasksQueue.add(tableBucket);
                        inProgressRebalanceTasks.put(
                                tableBucket,
                                RebalanceResultForBucket.of(planForBucket, NOT_STARTED));
                    }
                }));

        if (!inProgressRebalanceTasksQueue.isEmpty()) {
            // Trigger one rebalance task to execute.
            rebalanceStatus = REBALANCING;
            processNewRebalanceTask();
        } else {
            rebalanceStatus = newStatus;
        }
    }

    public void finishRebalanceTask(TableBucket tableBucket, RebalanceStatus statusForBucket) {
        checkNotClosed();
        if (inProgressRebalanceTasksQueue.contains(tableBucket)) {
            inProgressRebalanceTasksQueue.remove(tableBucket);
            RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.remove(tableBucket);
            checkNotNull(resultForBucket, "RebalanceResultForBucket is null.");
            finishedRebalanceTasks.put(
                    tableBucket,
                    RebalanceResultForBucket.of(resultForBucket.plan(), statusForBucket));
            LOG.info(
                    "Rebalance task {} in progress: {} tasks pending, {} completed.",
                    currentRebalanceId,
                    inProgressRebalanceTasksQueue.size(),
                    finishedRebalanceTasks.size());

            if (inProgressRebalanceTasksQueue.isEmpty()) {
                // All rebalance tasks are completed.
                completeRebalance();
            } else {
                // Trigger one rebalance task to execute.
                processNewRebalanceTask();
            }
        }
    }

    public @Nullable RebalanceProgress listRebalanceProgress(@Nullable String rebalanceId) {
        checkNotClosed();
        if (rebalanceId != null
                && currentRebalanceId != null
                && !rebalanceId.equals(currentRebalanceId)) {
            LOG.warn(
                    "Ignore the list rebalance task because it is not the current"
                            + " rebalance task.");
            throw new NoRebalanceInProgressException(
                    String.format(
                            "Rebalance task id %s to list is not the current rebalance task id %s.",
                            rebalanceId, currentRebalanceId));
        }

        if (currentRebalanceId == null) {
            return null;
        }

        Map<TableBucket, RebalanceResultForBucket> progressForBucketMap = new HashMap<>();
        progressForBucketMap.putAll(inProgressRebalanceTasks);
        progressForBucketMap.putAll(finishedRebalanceTasks);
        // the progress will be set at client.
        return new RebalanceProgress(
                currentRebalanceId, rebalanceStatus, 0.0, progressForBucketMap);
    }

    public void cancelRebalance(@Nullable String rebalanceId) {
        checkNotClosed();

        if (rebalanceId != null
                && currentRebalanceId != null
                && !rebalanceId.equals(currentRebalanceId)) {
            // do nothing.
            LOG.warn(
                    "Ignore the cancel rebalance task because it is not the current"
                            + " rebalance task.");
            throw new NoRebalanceInProgressException(
                    String.format(
                            "Rebalance task id %s to cancel is not the current rebalance task id %s.",
                            rebalanceId, currentRebalanceId));
        }

        if (rebalanceStatus != null && FINAL_STATUSES.contains(rebalanceStatus)) {
            // do nothing for the final state rebalance task.
            return;
        }

        try {
            Optional<RebalanceTask> rebalanceTaskOpt = zkClient.getRebalanceTask();
            if (rebalanceTaskOpt.isPresent()) {
                RebalanceTask rebalanceTask = rebalanceTaskOpt.get();
                zkClient.registerRebalanceTask(
                        new RebalanceTask(
                                rebalanceTask.getRebalanceId(),
                                CANCELED,
                                rebalanceTask.getExecutePlan()));
            }
        } catch (Exception e) {
            LOG.error("Error when delete rebalance plan from zookeeper.", e);
        }

        rebalanceStatus = CANCELED;
        inProgressRebalanceTasksQueue.clear();
        inProgressRebalanceTasks.clear();
        // Here, it will not clear finishedRebalanceTasks, because it will be used by
        // listRebalanceProgress. It will be cleared when next register.

        LOG.info("Cancel rebalance task success.");
    }

    public boolean hasInProgressRebalance() {
        checkNotClosed();
        return !inProgressRebalanceTasks.isEmpty() || !inProgressRebalanceTasksQueue.isEmpty();
    }

    public RebalanceTask generateRebalanceTask(List<Goal> goalsByPriority) {
        checkNotClosed();
        List<RebalancePlanForBucket> rebalancePlanForBuckets;
        String rebalanceId = UUID.randomUUID().toString();
        try {
            // Generate the latest cluster model.
            long startTime = System.currentTimeMillis();
            ClusterModel clusterModel = buildClusterModel(eventProcessor.getCoordinatorContext());
            LOG.info(
                    "Build cluster model for rebalance id {} with {} ms.",
                    rebalanceId,
                    System.currentTimeMillis() - startTime);

            // do optimize.
            startTime = System.currentTimeMillis();
            rebalancePlanForBuckets = goalOptimizer.doOptimizeOnce(clusterModel, goalsByPriority);
            LOG.info(
                    "Do optimize for rebalance id {} with {} ms.",
                    rebalanceId,
                    System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            LOG.error("Failed to generate rebalance plan.", e);
            throw e;
        }

        // group by tableId and partitionId to generate rebalance plan.
        return buildRebalanceTask(rebalanceId, rebalancePlanForBuckets);
    }

    public @Nullable RebalancePlanForBucket getRebalancePlanForBucket(TableBucket tableBucket) {
        checkNotClosed();
        RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.get(tableBucket);
        if (resultForBucket != null) {
            return resultForBucket.plan();
        }
        return null;
    }

    private void processNewRebalanceTask() {
        TableBucket tableBucket = inProgressRebalanceTasksQueue.peek();
        if (tableBucket != null && inProgressRebalanceTasks.containsKey(tableBucket)) {
            RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.get(tableBucket);
            RebalanceResultForBucket rebalanceResultForBucket =
                    RebalanceResultForBucket.of(resultForBucket.plan(), REBALANCING);
            eventProcessor.tryToExecuteRebalanceTask(rebalanceResultForBucket.plan());
        }
    }

    private void completeRebalance() {
        checkNotClosed();
        try {
            Optional<RebalanceTask> rebalanceTaskOpt = zkClient.getRebalanceTask();
            Map<TableBucket, RebalancePlanForBucket> bucketPlan;
            if (rebalanceTaskOpt.isPresent()) {
                bucketPlan = rebalanceTaskOpt.get().getExecutePlan();
            } else {
                LOG.warn(
                        "Rebalance task is empty in zk when complete rebalance. "
                                + "It will be treated as no rebalance tasks.");
                bucketPlan = new HashMap<>();
            }
            zkClient.registerRebalanceTask(
                    new RebalanceTask(currentRebalanceId, COMPLETED, bucketPlan));
        } catch (Exception e) {
            LOG.error("Error when update rebalance plan from zookeeper.", e);
        }

        rebalanceStatus = COMPLETED;
        inProgressRebalanceTasks.clear();
        inProgressRebalanceTasksQueue.clear();

        // Here, it will not clear finishedRebalanceTasks, because it will be used by
        // listRebalanceProgress. It will be cleared when next register.

        LOG.info("Rebalance complete with {} ms.", System.currentTimeMillis() - registerTime);
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
                        id, new ServerModel(id, rack, isOfflineTagged(serverTags.get(id))));
            } else {
                serverModelMap.put(id, new ServerModel(id, rack, false));
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

    private RebalanceTask buildRebalanceTask(
            String rebalanceId, List<RebalancePlanForBucket> rebalancePlanForBuckets) {
        Map<TableBucket, RebalancePlanForBucket> bucketPlan = new HashMap<>();
        for (RebalancePlanForBucket rebalancePlanForBucket : rebalancePlanForBuckets) {
            bucketPlan.put(rebalancePlanForBucket.getTableBucket(), rebalancePlanForBucket);
        }
        return new RebalanceTask(rebalanceId, NOT_STARTED, bucketPlan);
    }

    private boolean isOfflineTagged(ServerTag serverTag) {
        return serverTag == ServerTag.PERMANENT_OFFLINE || serverTag == ServerTag.TEMPORARY_OFFLINE;
    }

    private ClusterModel initialClusterModel(Map<Integer, ServerModel> serverModelMap) {
        SortedSet<ServerModel> servers = new TreeSet<>(serverModelMap.values());
        return new ClusterModel(servers);
    }

    private void checkNotClosed() {
        checkArgument(!isClosed, "RebalanceManager is already closed.");
    }

    public void close() {
        isClosed = true;
    }

    @VisibleForTesting
    public ClusterModel buildClusterModel() {
        return buildClusterModel(eventProcessor.getCoordinatorContext());
    }

    @VisibleForTesting
    @Nullable
    RebalanceStatus getRebalanceStatus() {
        return rebalanceStatus;
    }
}
