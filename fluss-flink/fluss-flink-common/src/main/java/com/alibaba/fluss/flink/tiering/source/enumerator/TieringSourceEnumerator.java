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

package com.alibaba.fluss.flink.tiering.source.enumerator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.metrics.FlinkMetricRegistry;
import com.alibaba.fluss.flink.tiering.event.FailedTieringEvent;
import com.alibaba.fluss.flink.tiering.event.FinishedTieringEvent;
import com.alibaba.fluss.flink.tiering.source.split.TieringSplit;
import com.alibaba.fluss.flink.tiering.source.split.TieringSplitGenerator;
import com.alibaba.fluss.flink.tiering.source.state.TieringSourceEnumeratorState;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import com.alibaba.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import com.alibaba.fluss.rpc.messages.PbHeartbeatReqForTable;
import com.alibaba.fluss.rpc.messages.PbLakeTieringTableInfo;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.utils.MapUtils;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.alibaba.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator.HeartBeatHelper.basicHeartBeat;
import static com.alibaba.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator.HeartBeatHelper.failedTableHeartBeat;
import static com.alibaba.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator.HeartBeatHelper.heartBeatWithRequestNewTieringTable;
import static com.alibaba.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator.HeartBeatHelper.tieringTableHeartBeat;
import static com.alibaba.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator.HeartBeatHelper.waitHeartbeatResponse;

/**
 * An implementation of {@link SplitEnumerator} used to request {@link TieringSplit} from Fluss
 * Cluster.
 *
 * <p>The enumerator is responsible for:
 *
 * <ul>
 *   <li>Register the Tiering Service job that the current TieringSourceEnumerator belongs to with
 *       the Fluss Cluster when the Flink Tiering job starts up.
 *   <li>Request Fluss table splits from Fluss Cluster and assigns to SourceReader to tier.
 *   <li>Un-Register the Tiering Service job that the current TieringSourceEnumerator belongs to
 *       with the Fluss Cluster when the Flink Tiering job shutdown as much as possible.
 * </ul>
 */
public class TieringSourceEnumerator
        implements SplitEnumerator<TieringSplit, TieringSourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(TieringSourceEnumerator.class);

    private final Configuration flussConf;
    private final SplitEnumeratorContext<TieringSplit> context;
    private final SplitEnumeratorMetricGroup enumeratorMetricGroup;
    private final long pollTieringTableIntervalMs;
    private final List<TieringSplit> pendingSplits;
    private final Set<Integer> readersAwaitingSplit;
    private final Map<Long, Long> tieringTableEpochs;
    private final Map<Long, Long> failedTableEpochs;
    private final Map<Long, Long> finishedTableEpochs;

    // lazily instantiated
    private RpcClient rpcClient;
    private CoordinatorGateway coordinatorGateway;
    private Connection connection;
    private Admin flussAdmin;
    private TieringSplitGenerator splitGenerator;
    private int flussCoordinatorEpoch;

    public TieringSourceEnumerator(
            Configuration flussConf,
            SplitEnumeratorContext<TieringSplit> context,
            long pollTieringTableIntervalMs) {
        this.flussConf = flussConf;
        this.context = context;
        this.enumeratorMetricGroup = context.metricGroup();
        this.pollTieringTableIntervalMs = pollTieringTableIntervalMs;
        this.pendingSplits = new ArrayList<>();
        this.readersAwaitingSplit = new TreeSet<>();
        this.tieringTableEpochs = MapUtils.newConcurrentHashMap();
        this.finishedTableEpochs = MapUtils.newConcurrentHashMap();
        this.failedTableEpochs = MapUtils.newConcurrentHashMap();
    }

    @Override
    public void start() {
        connection = ConnectionFactory.createConnection(flussConf);
        flussAdmin = connection.getAdmin();
        FlinkMetricRegistry metricRegistry = new FlinkMetricRegistry(enumeratorMetricGroup);
        ClientMetricGroup clientMetricGroup =
                new ClientMetricGroup(metricRegistry, "LakeTieringService");
        this.rpcClient = RpcClient.create(flussConf, clientMetricGroup, false);
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussConf, rpcClient);
        this.coordinatorGateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
        this.splitGenerator = new TieringSplitGenerator(flussAdmin);

        LOG.info("Starting register Tiering Service to Fluss Coordinator...");
        try {
            LakeTieringHeartbeatResponse heartbeatResponse =
                    waitHeartbeatResponse(
                            coordinatorGateway.lakeTieringHeartbeat(basicHeartBeat()));
            this.flussCoordinatorEpoch = heartbeatResponse.getCoordinatorEpoch();
            LOG.info(
                    "Register Tiering Service to Fluss Coordinator(epoch={}) success.",
                    flussCoordinatorEpoch);

        } catch (Exception e) {
            LOG.error("Register Tiering Service failed due to ", e);
            throw new FlinkRuntimeException("Register Tiering Service failed due to ", e);
        }
        this.context.callAsync(
                this::requestTieringTableSplitsViaHeartBeat,
                this::generateAndAssignSplits,
                0,
                pollTieringTableIntervalMs);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader may be failed, skip this request.
            return;
        }
        LOG.info("TiringSourceReader {} requests split.", subtaskId);
        readersAwaitingSplit.add(subtaskId);
        this.context.callAsync(
                this::requestTieringTableSplitsViaHeartBeat, this::generateAndAssignSplits);
    }

    @Override
    public void addSplitsBack(List<TieringSplit> splits, int subtaskId) {
        readersAwaitingSplit.add(subtaskId);
        pendingSplits.addAll(splits);
        assignSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Adding reader: {} to Tiering Source enumerator.", subtaskId);
        if (context.registeredReaders().containsKey(subtaskId)) {
            readersAwaitingSplit.add(subtaskId);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedTieringEvent) {
            FinishedTieringEvent finishedTieringEvent = (FinishedTieringEvent) sourceEvent;
            long finishedTableId = finishedTieringEvent.getTableId();
            Long tieringEpoch = tieringTableEpochs.remove(finishedTableId);
            if (tieringEpoch == null) {
                // shouldn't happen, warn it
                LOG.warn(
                        "The finished table {} is not in tiering table, won't report it to Fluss to mark as finished.",
                        finishedTableId);
            } else {
                finishedTableEpochs.put(finishedTableId, tieringEpoch);
            }
        }

        if (sourceEvent instanceof FailedTieringEvent) {
            FailedTieringEvent failedEvent = (FailedTieringEvent) sourceEvent;
            long failedTableId = failedEvent.getTableId();
            Long tieringEpoch = tieringTableEpochs.remove(failedTableId);
            LOG.info(
                    "Tiering table {} is failed, fail reason is {}.",
                    failedTableId,
                    failedEvent.failReason());
            if (tieringEpoch == null) {
                // shouldn't happen, warn it
                LOG.warn(
                        "The failed table {} is not in tiering table, won't report it to Fluss to mark as failed.",
                        failedTableId);
            } else {
                failedTableEpochs.put(failedTableId, tieringEpoch);
            }
        }

        if (!finishedTableEpochs.isEmpty() || !failedTableEpochs.isEmpty()) {
            // call one round of heartbeat to notify table has been finished or failed
            this.context.callAsync(
                    this::requestTieringTableSplitsViaHeartBeat, this::generateAndAssignSplits);
        }
    }

    private void generateAndAssignSplits(
            @Nullable Tuple3<Long, Long, TablePath> tieringTable, Throwable throwable) {
        if (throwable != null) {
            LOG.warn("Failed to request tiering table, will retry later.", throwable);
        }
        if (tieringTable != null) {
            generateTieringSplits(tieringTable);
        }
        assignSplits();
    }

    private void assignSplits() {
        /* This method may be called from both addSplitsBack and handleSplitRequest, make it thread safe. */
        synchronized (readersAwaitingSplit) {
            if (!readersAwaitingSplit.isEmpty()) {
                final Integer[] readers = readersAwaitingSplit.toArray(new Integer[0]);
                for (Integer nextAwaitingReader : readers) {
                    if (!context.registeredReaders().containsKey(nextAwaitingReader)) {
                        readersAwaitingSplit.remove(nextAwaitingReader);
                        continue;
                    }
                    if (!pendingSplits.isEmpty()) {
                        TieringSplit tieringSplit = pendingSplits.remove(0);
                        context.assignSplit(tieringSplit, nextAwaitingReader);
                        readersAwaitingSplit.remove(nextAwaitingReader);
                    }
                }
            }
        }
    }

    private @Nullable Tuple3<Long, Long, TablePath> requestTieringTableSplitsViaHeartBeat() {
        Map<Long, Long> currentFinishedTableEpochs = new HashMap<>(this.finishedTableEpochs);
        Map<Long, Long> currentFailedTableEpochs = new HashMap<>(this.failedTableEpochs);
        LakeTieringHeartbeatRequest tieringHeartbeatRequest =
                tieringTableHeartBeat(
                        basicHeartBeat(),
                        this.tieringTableEpochs,
                        currentFinishedTableEpochs,
                        currentFailedTableEpochs,
                        this.flussCoordinatorEpoch);

        Tuple3<Long, Long, TablePath> lakeTieringInfo = null;

        if (pendingSplits.isEmpty() && !readersAwaitingSplit.isEmpty()) {
            // report heartbeat with request table to fluss coordinator
            LakeTieringHeartbeatResponse heartbeatResponse =
                    waitHeartbeatResponse(
                            coordinatorGateway.lakeTieringHeartbeat(
                                    heartBeatWithRequestNewTieringTable(tieringHeartbeatRequest)));
            if (heartbeatResponse.hasTieringTable()) {
                PbLakeTieringTableInfo tieringTable = heartbeatResponse.getTieringTable();
                lakeTieringInfo =
                        Tuple3.of(
                                tieringTable.getTableId(),
                                tieringTable.getTieringEpoch(),
                                TablePath.of(
                                        tieringTable.getTablePath().getDatabaseName(),
                                        tieringTable.getTablePath().getTableName()));
            } else {
                LOG.info("No available Tiering table found, will poll later.");
            }
        } else {
            // report heartbeat to fluss coordinator
            waitHeartbeatResponse(coordinatorGateway.lakeTieringHeartbeat(tieringHeartbeatRequest));
        }

        // if come to here, we can remove currentFinishedTableEpochs/failedTableEpochs to avoid send
        // in next round
        currentFinishedTableEpochs.forEach(finishedTableEpochs::remove);
        currentFailedTableEpochs.forEach(failedTableEpochs::remove);
        return lakeTieringInfo;
    }

    private void generateTieringSplits(Tuple3<Long, Long, TablePath> tieringTable)
            throws FlinkRuntimeException {
        if (tieringTable == null) {
            return;
        }
        long start = System.currentTimeMillis();
        LOG.info("Generate Tiering splits for table {}.", tieringTable.f2);
        try {
            List<TieringSplit> tieringSplits =
                    populateNumberOfTieringSplits(
                            splitGenerator.generateTableSplits(tieringTable.f2));
            LOG.info(
                    "Generate Tiering {} splits for table {} with cost {}ms.",
                    tieringSplits.size(),
                    tieringTable.f2,
                    System.currentTimeMillis() - start);
            if (tieringSplits.isEmpty()) {
                LOG.info(
                        "Generate Tiering splits for table {} is empty, no need to tier data.",
                        tieringTable.f2.getTableName());
                finishedTableEpochs.put(tieringTable.f0, tieringTable.f1);
            } else {
                tieringTableEpochs.put(tieringTable.f0, tieringTable.f1);
                pendingSplits.addAll(tieringSplits);
            }
        } catch (Exception e) {
            LOG.warn("Fail to generate Tiering splits for table {}.", tieringTable.f2, e);
            failedTableEpochs.put(tieringTable.f0, tieringTable.f1);
        }
    }

    private List<TieringSplit> populateNumberOfTieringSplits(List<TieringSplit> tieringSplits) {
        int numberOfSplits = tieringSplits.size();
        return tieringSplits.stream()
                .map(split -> split.copy(numberOfSplits))
                .collect(Collectors.toList());
    }

    @Override
    public TieringSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        // do nothing, the downstream lake commiter will snapshot the state to Fluss Cluster
        return new TieringSourceEnumeratorState();
    }

    @Override
    public void close() throws IOException {
        if (rpcClient != null) {
            failedTableEpochs.putAll(tieringTableEpochs);
            tieringTableEpochs.clear();
            if (!failedTableEpochs.isEmpty()) {
                reportFailedTable(basicHeartBeat(), failedTableEpochs);
            }
            try {
                LOG.info("Closing Tiering Source Enumerator of at {}.", System.currentTimeMillis());
                rpcClient.close();
            } catch (Exception e) {
                LOG.error("Failed to close Tiering Source enumerator.", e);
            }
        }
        try {
            if (flussAdmin != null) {
                LOG.info("Closing Fluss Admin client...");
                flussAdmin.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to close Fluss Admin client.", e);
        }
        try {
            if (connection != null) {
                LOG.info("Closing Fluss connection...");
                connection.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to close Fluss connection.", e);
        }
    }

    /**
     * Report failed table to Fluss coordinator via HeartBeat, this method should be called when
     * {@link TieringSourceEnumerator} is closed or receives failed table from downstream lake
     * committer.
     */
    private void reportFailedTable(
            LakeTieringHeartbeatRequest heartbeatRequest, Map<Long, Long> failedTableEpochs)
            throws FlinkRuntimeException {
        try {
            waitHeartbeatResponse(
                    coordinatorGateway.lakeTieringHeartbeat(
                            failedTableHeartBeat(
                                    heartbeatRequest, failedTableEpochs, flussCoordinatorEpoch)));
            LOG.info("Report failed table to Fluss Coordinator success");

        } catch (Exception e) {
            LOG.error("Errors happens when report failed table to Fluss cluster.", e);
            throw new FlinkRuntimeException(
                    "Errors happens when report failed table to Fluss cluster.", e);
        }
    }

    /** A helper class to build heartbeat request. */
    @VisibleForTesting
    static class HeartBeatHelper {

        static LakeTieringHeartbeatRequest basicHeartBeat() {
            return new LakeTieringHeartbeatRequest();
        }

        static LakeTieringHeartbeatRequest heartBeatWithRequestNewTieringTable(
                LakeTieringHeartbeatRequest heartbeatRequest) {
            heartbeatRequest.setRequestTable(true);
            return heartbeatRequest;
        }

        static LakeTieringHeartbeatRequest tieringTableHeartBeat(
                LakeTieringHeartbeatRequest heartbeatRequest,
                Map<Long, Long> tieringTableEpochs,
                Map<Long, Long> finishedTableEpochs,
                Map<Long, Long> failedTableEpochs,
                int coordinatorEpoch) {
            if (!tieringTableEpochs.isEmpty()) {
                heartbeatRequest.addAllTieringTables(
                        toPbHeartbeatReqForTable(tieringTableEpochs, coordinatorEpoch));
            }
            if (!finishedTableEpochs.isEmpty()) {
                heartbeatRequest.addAllFinishedTables(
                        toPbHeartbeatReqForTable(finishedTableEpochs, coordinatorEpoch));
            }
            // add failed tiering table to heart beat request
            return failedTableHeartBeat(heartbeatRequest, failedTableEpochs, coordinatorEpoch);
        }

        private static Set<PbHeartbeatReqForTable> toPbHeartbeatReqForTable(
                Map<Long, Long> tableEpochs, int coordinatorEpoch) {
            return tableEpochs.entrySet().stream()
                    .map(
                            tieringTableEpoch ->
                                    new PbHeartbeatReqForTable()
                                            .setTableId(tieringTableEpoch.getKey())
                                            .setCoordinatorEpoch(coordinatorEpoch)
                                            .setTieringEpoch(tieringTableEpoch.getValue()))
                    .collect(Collectors.toSet());
        }

        /**
         * Report failed table to Fluss coordinator via HeartBeat, this method should be called when
         * {@link TieringSourceEnumerator} is closed or receives failed table from downstream lake
         * committer.
         */
        static LakeTieringHeartbeatRequest failedTableHeartBeat(
                LakeTieringHeartbeatRequest lakeTieringHeartbeatRequest,
                Map<Long, Long> failedTieringTableEpochs,
                int coordinatorEpoch) {
            if (!failedTieringTableEpochs.isEmpty()) {
                lakeTieringHeartbeatRequest.addAllFailedTables(
                        toPbHeartbeatReqForTable(failedTieringTableEpochs, coordinatorEpoch));
            }
            return lakeTieringHeartbeatRequest;
        }

        static LakeTieringHeartbeatResponse waitHeartbeatResponse(
                CompletableFuture<LakeTieringHeartbeatResponse> responseCompletableFuture) {
            try {
                return responseCompletableFuture.get();
            } catch (Exception e) {
                LOG.error("Failed to wait heartbeat response due to ", e);
                throw new FlinkRuntimeException("Failed to wait heartbeat response due to ", e);
            }
        }
    }
}
