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

package com.alibaba.fluss.client.metadata;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.client.utils.ClientUtils;
import com.alibaba.fluss.cluster.BucketLocation;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.RetriableException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.AdminReadOnlyGateway;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.alibaba.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;

/** The updater to initialize and update client metadata. */
public class MetadataUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataUpdater.class);

    private static final int MAX_RETRY_TIMES = 5;

    private final RpcClient rpcClient;
    protected volatile Cluster cluster;

    public MetadataUpdater(Configuration configuration, RpcClient rpcClient) {
        this(rpcClient, initializeCluster(configuration, rpcClient));
    }

    @VisibleForTesting
    public MetadataUpdater(RpcClient rpcClient, Cluster cluster) {
        this.rpcClient = rpcClient;
        this.cluster = cluster;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public @Nullable ServerNode getCoordinatorServer() {
        return cluster.getCoordinatorServer();
    }

    public long getTableId(TablePath tablePath) {
        return cluster.getTableId(tablePath);
    }

    public Long getPartitionIdOrElseThrow(PhysicalTablePath physicalTablePath) {
        return cluster.getPartitionIdOrElseThrow(physicalTablePath);
    }

    public String getPartitionNameOrElseThrow(long partitionId) {
        return cluster.getPartitionNameOrElseThrow(partitionId);
    }

    public TableInfo getTableInfoOrElseThrow(TablePath tablePath) {
        return cluster.getTableOrElseThrow(tablePath);
    }

    public Optional<BucketLocation> getBucketLocation(TableBucket tableBucket) {
        return cluster.getBucketLocation(tableBucket);
    }

    private Optional<TableInfo> getTableInfo(TablePath tablePath) {
        return cluster.getTable(tablePath);
    }

    public TableInfo getTableInfoOrElseThrow(long tableId) {
        return getTableInfo(cluster.getTablePathOrElseThrow(tableId))
                .orElseThrow(
                        () ->
                                new FlussRuntimeException(
                                        "Table not found for table id: " + tableId));
    }

    public int leaderFor(TableBucket tableBucket) {
        ServerNode serverNode = cluster.leaderFor(tableBucket);
        if (serverNode == null) {
            for (int i = 0; i < MAX_RETRY_TIMES; i++) {
                TablePath tablePath = cluster.getTablePathOrElseThrow(tableBucket.getTableId());
                // check if bucket is for a partition
                if (tableBucket.getPartitionId() != null) {
                    updateMetadata(
                            Collections.singleton(tablePath),
                            null,
                            Collections.singleton(tableBucket.getPartitionId()));
                } else {
                    updateMetadata(Collections.singleton(tablePath), null, null);
                }
                serverNode = cluster.leaderFor(tableBucket);
                if (serverNode != null) {
                    break;
                }
            }

            if (serverNode == null) {
                throw new FlussRuntimeException(
                        "Leader not found after retry  "
                                + MAX_RETRY_TIMES
                                + " times for table bucket: "
                                + tableBucket);
            }
        }

        return serverNode.id();
    }

    public @Nullable ServerNode getTabletServer(int id) {
        return cluster.getTabletServer(id);
    }

    public @Nullable ServerNode getRandomTabletServer() {
        return cluster.getRandomTabletServer();
    }

    public CoordinatorGateway newCoordinatorServerClient() {
        return GatewayClientProxy.createGatewayProxy(
                this::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
    }

    public TabletServerGateway newRandomTabletServerClient() {
        return GatewayClientProxy.createGatewayProxy(
                this::getRandomTabletServer, rpcClient, TabletServerGateway.class);
    }

    public TabletServerGateway newTabletServerClientForNode(int serverId) {
        final ServerNode serverNode = getTabletServer(serverId);
        return GatewayClientProxy.createGatewayProxy(
                () -> serverNode, rpcClient, TabletServerGateway.class);
    }

    public void checkAndUpdateTableMetadata(Set<TablePath> tablePaths) {
        Set<TablePath> needUpdateTablePaths =
                tablePaths.stream()
                        .filter(tablePath -> !cluster.getTable(tablePath).isPresent())
                        .collect(Collectors.toSet());
        if (!needUpdateTablePaths.isEmpty()) {
            updateMetadata(needUpdateTablePaths, null, null);
        }
    }

    /**
     * Check the partition exists in metadata cache, if not, try to update the metadata cache, if
     * not exist yet, throw exception.
     *
     * <p>and update partition metadata .
     */
    public void checkAndUpdatePartitionMetadata(PhysicalTablePath physicalTablePath) {
        if (!cluster.getPartitionId(physicalTablePath).isPresent()) {
            updateMetadata(null, Collections.singleton(physicalTablePath), null);
        }
    }

    /**
     * Check the table/partition info for the given table bucket exist in metadata cache, if not,
     * try to update the metadata cache.
     */
    public void checkAndUpdateMetadata(TablePath tablePath, TableBucket tableBucket) {
        if (tableBucket.getPartitionId() == null) {
            checkAndUpdateTableMetadata(Collections.singleton(tablePath));
        } else {
            checkAndUpdatePartitionMetadata(
                    tablePath, Collections.singleton(tableBucket.getPartitionId()));
        }
    }

    /**
     * Check the partitions info for the given partition ids exist in metadata cache, if not, try to
     * update the metadata cache.
     *
     * <p>Note: it'll assume the partition ids belong to the given {@code tablePath}
     */
    public void checkAndUpdatePartitionMetadata(
            TablePath tablePath, Collection<Long> partitionIds) {
        Set<Long> needUpdatePartitionIds = new HashSet<>();
        for (Long partitionId : partitionIds) {
            if (!cluster.getPartitionName(partitionId).isPresent()) {
                needUpdatePartitionIds.add(partitionId);
            }
        }

        if (!needUpdatePartitionIds.isEmpty()) {
            updateMetadata(Collections.singleton(tablePath), null, needUpdatePartitionIds);
        }
    }

    public void updateTableOrPartitionMetadata(TablePath tablePath, @Nullable Long partitionId) {
        Collection<Long> partitionIds =
                partitionId == null ? null : Collections.singleton(partitionId);
        updateMetadata(Collections.singleton(tablePath), null, partitionIds);
    }

    /** Update the table or partition metadata info. */
    public void updatePhysicalTableMetadata(Set<PhysicalTablePath> physicalTablePaths) {
        Set<TablePath> updateTablePaths = new HashSet<>();
        Set<PhysicalTablePath> updatePartitionPath = new HashSet<>();
        for (PhysicalTablePath physicalTablePath : physicalTablePaths) {
            if (physicalTablePath.getPartitionName() == null) {
                updateTablePaths.add(physicalTablePath.getTablePath());
            } else {
                updatePartitionPath.add(physicalTablePath);
            }
        }
        updateMetadata(updateTablePaths, updatePartitionPath, null);
    }

    @VisibleForTesting
    protected void updateMetadata(
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitionNames,
            @Nullable Collection<Long> tablePartitionIds) {
        try {
            synchronized (this) {
                cluster =
                        sendMetadataRequestAndRebuildCluster(
                                cluster,
                                rpcClient,
                                tablePaths,
                                tablePartitionNames,
                                tablePartitionIds);
            }
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (t instanceof RetriableException || t instanceof TimeoutException) {
                LOG.warn("Failed to update metadata, but the exception is re-triable.", t);
            } else {
                throw new FlussRuntimeException("Failed to update metadata", t);
            }
        }
    }

    /**
     * Initialize Cluster. This step just to get the coordinator server address and alive tablet
     * servers according to the config {@link ConfigOptions#BOOTSTRAP_SERVERS}.
     */
    private static Cluster initializeCluster(Configuration conf, RpcClient rpcClient) {
        List<InetSocketAddress> inetSocketAddresses =
                ClientUtils.parseAndValidateAddresses(conf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        Cluster cluster = null;
        Exception lastException = null;
        for (InetSocketAddress address : inetSocketAddresses) {
            try {
                cluster = tryToInitializeCluster(rpcClient, address);
                break;
            } catch (Exception e) {
                LOG.error(
                        "Failed to initialize fluss client connection to bootstrap server: {}",
                        address,
                        e);
                lastException = e;
            }
        }

        if (cluster == null && lastException != null) {
            String errorMsg =
                    "Failed to initialize fluss client connection to server because no "
                            + "bootstrap server is validate. bootstrap servers: "
                            + inetSocketAddresses;
            LOG.error(errorMsg);
            throw new IllegalStateException(errorMsg, lastException);
        }

        return cluster;
    }

    private static Cluster tryToInitializeCluster(RpcClient rpcClient, InetSocketAddress address)
            throws Exception {
        ServerNode serverNode =
                new ServerNode(
                        -1, address.getHostString(), address.getPort(), ServerType.COORDINATOR);
        AdminReadOnlyGateway adminReadOnlyGateway =
                GatewayClientProxy.createGatewayProxy(
                        () -> serverNode, rpcClient, AdminReadOnlyGateway.class);
        return sendMetadataRequestAndRebuildCluster(adminReadOnlyGateway, Collections.emptySet());
    }

    /** Invalid the bucket metadata for the given physical table paths. */
    public void invalidPhysicalTableBucketMeta(Set<PhysicalTablePath> physicalTablesToInvalid) {
        if (!physicalTablesToInvalid.isEmpty()) {
            cluster = cluster.invalidPhysicalTableBucketMeta(physicalTablesToInvalid);
        }
    }

    /** Get the table physical paths by table ids and partition ids. */
    public Set<PhysicalTablePath> getPhysicalTablePathByIds(
            @Nullable Collection<Long> tableId,
            @Nullable Collection<TablePartition> tablePartitions) {
        Set<PhysicalTablePath> physicalTablePaths = new HashSet<>();
        if (tableId != null) {
            tableId.forEach(
                    id ->
                            cluster.getTablePath(id)
                                    .ifPresent(
                                            p -> physicalTablePaths.add(PhysicalTablePath.of(p))));
        }

        if (tablePartitions != null) {
            for (TablePartition tablePartition : tablePartitions) {
                cluster.getTablePath(tablePartition.getTableId())
                        .ifPresent(
                                path -> {
                                    Optional<String> optPartition =
                                            cluster.getPartitionName(
                                                    tablePartition.getPartitionId());
                                    optPartition.ifPresent(
                                            p ->
                                                    physicalTablePaths.add(
                                                            PhysicalTablePath.of(path, p)));
                                });
            }
        }
        return physicalTablePaths;
    }
}
