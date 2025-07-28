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
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.RebalanceResponse;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.rebalance.executor.RebalanceActionExecutorService;
import org.apache.fluss.server.coordinator.rebalance.goal.Goal;
import org.apache.fluss.server.coordinator.rebalance.goal.GoalOptimizer;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.RackModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RebalancePlan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/** A rebalance manager to generate rebalance plan, and execution rebalance plan. */
public class RebalanceManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Lock lock = new ReentrantLock();
    private final RebalanceActionExecutorService actionExecutorService;
    private final ZooKeeperClient zkClient;
    private final Supplier<EventManager> eventManagerSupplier;

    @GuardedBy("lock")
    private final GoalOptimizer goalOptimizer;

    @GuardedBy("lock")
    private RebalanceStatus rebalanceStatus;

    @GuardedBy("lock")
    private @Nullable Throwable rebalanceFailureCause = null;

    public RebalanceManager(Supplier<EventManager> eventManagerSupplier, ZooKeeperClient zkClient) {
        this.eventManagerSupplier = eventManagerSupplier;
        this.actionExecutorService = new RebalanceActionExecutorService(eventManagerSupplier);
        actionExecutorService.start();

        this.zkClient = zkClient;
        this.goalOptimizer = new GoalOptimizer();

        registerRebalanceTasksFromZookeeper();
    }

    private void registerRebalanceTasksFromZookeeper() {
        try {
            Optional<RebalancePlan> rebalancePlanOpt = zkClient.getRebalancePlan();
            if (rebalancePlanOpt.isPresent()) {
                rebalanceStatus = RebalanceStatus.TASK_EXECUTING;
                // TODO trigger to execution.
            } else {
                rebalanceStatus = RebalanceStatus.NO_ONGOING_REBALANCE;
            }
        } catch (Exception e) {
            LOG.error(
                    "Failed to get rebalance plan from zookeeper, it will be treated as no "
                            + "ongoing rebalancing plan.",
                    e);
        }
    }

    public boolean hasOngoingRebalance() {
        checkNotClosed();
        return inLock(
                lock,
                () ->
                        rebalanceStatus == RebalanceStatus.PLAN_GENERATING
                                || rebalanceStatus == RebalanceStatus.TASK_EXECUTING);
    }

    public RebalancePlan generateRebalancePlan(List<Goal> goalsByPriority) throws Exception {
        checkNotClosed();
        return inLock(
                lock,
                () -> {
                    rebalanceStatus = RebalanceStatus.PLAN_GENERATING;

                    List<RebalancePlanForBucket> rebalancePlanForBuckets;
                    try {
                        // Generate the latest cluster model.
                        ClusterModel clusterModel = getClusterModel();

                        // do optimize.
                        rebalancePlanForBuckets =
                                goalOptimizer.doOptimizeOnce(clusterModel, goalsByPriority);
                    } catch (Exception e) {
                        LOG.error("Failed to generate rebalance plan.", e);
                        rebalanceStatus = RebalanceStatus.FAILED;
                        throw e;
                    }

                    rebalanceStatus = RebalanceStatus.NO_ONGOING_REBALANCE;
                    // group by tableId and partitionId to generate rebalance plan.
                    return buildRebalancePlan(rebalancePlanForBuckets);
                });
    }

    public CompletableFuture<RebalanceResponse> executeRebalancePlan(RebalancePlan rebalancePlan) {
        checkNotClosed();
        return inLock(
                lock,
                () -> {
                    rebalanceStatus = RebalanceStatus.TASK_EXECUTING;
                    return actionExecutorService
                            .execute(rebalancePlan.getExecutePlan())
                            .whenComplete(
                                    (unused, throwable) -> {
                                        if (throwable == null) {
                                            rebalanceStatus = RebalanceStatus.COMPLETED;
                                        } else {
                                            rebalanceStatus = RebalanceStatus.FAILED;
                                        }
                                    });
                });
    }

    @VisibleForTesting
    public ClusterModel getClusterModel() throws Exception {
        AccessContextEvent<ClusterModel> accessContextEvent =
                new AccessContextEvent<>(this::buildClusterModel);
        eventManagerSupplier.get().put(accessContextEvent);
        return accessContextEvent.getResultFuture().get();
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
            LeaderAndIsr isr = coordinatorContext.getBucketLeaderAndIsr(tableBucket).get();
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
        return new RebalancePlan(bucketPlan);
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

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            actionExecutorService.shutdown();
        }
    }
}
