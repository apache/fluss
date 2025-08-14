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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.IllegalConfigurationException;
import com.alibaba.fluss.lake.lakestorage.LakeCatalog;
import com.alibaba.fluss.lake.lakestorage.LakeStorage;
import com.alibaba.fluss.lake.lakestorage.LakeStoragePlugin;
import com.alibaba.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.RpcServer;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.rpc.netty.server.RequestsMetrics;
import com.alibaba.fluss.server.ServerBase;
import com.alibaba.fluss.server.authorizer.Authorizer;
import com.alibaba.fluss.server.authorizer.AuthorizerLoader;
import com.alibaba.fluss.server.metadata.CoordinatorMetadataCache;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.metrics.ServerMetricUtils;
import com.alibaba.fluss.server.metrics.group.CoordinatorMetricGroup;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperUtils;
import com.alibaba.fluss.server.zk.data.CoordinatorAddress;
import com.alibaba.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.ExecutorUtils;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;
import com.alibaba.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Coordinator server implementation. The coordinator server is responsible to:
 *
 * <ul>
 *   <li>manage the tablet servers
 *   <li>manage the metadata
 *   <li>coordinate the whole cluster, e.g. data re-balance, recover data when tablet servers down
 * </ul>
 */
public class CoordinatorServer extends ServerBase {

    public static final String DEFAULT_DATABASE = "fluss";
    private static final String SERVER_NAME = "CoordinatorServer";

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorServer.class);

    /** The lock to guard startup / shutdown / manipulation methods. */
    private final Object lock = new Object();

    private final CompletableFuture<Result> terminationFuture;

    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    @GuardedBy("lock")
    private int serverId;

    @GuardedBy("lock")
    private MetricRegistry metricRegistry;

    @GuardedBy("lock")
    private CoordinatorMetricGroup serverMetricGroup;

    @GuardedBy("lock")
    private RpcServer rpcServer;

    @GuardedBy("lock")
    private RpcClient rpcClient;

    @GuardedBy("lock")
    private ClientMetricGroup clientMetricGroup;

    @GuardedBy("lock")
    private CoordinatorService coordinatorService;

    @GuardedBy("lock")
    private CoordinatorMetadataCache metadataCache;

    @GuardedBy("lock")
    private CoordinatorChannelManager coordinatorChannelManager;

    @GuardedBy("lock")
    private CoordinatorEventProcessor coordinatorEventProcessor;

    @GuardedBy("lock")
    private ZooKeeperClient zkClient;

    @GuardedBy("lock")
    private AutoPartitionManager autoPartitionManager;

    @GuardedBy("lock")
    private LakeTableTieringManager lakeTableTieringManager;

    @GuardedBy("lock")
    private ExecutorService ioExecutor;

    @GuardedBy("lock")
    @Nullable
    private Authorizer authorizer;

    @GuardedBy("lock")
    private CoordinatorContext coordinatorContext;

    public CoordinatorServer(Configuration conf) {
        super(conf);
        validateConfigs(conf);
        this.terminationFuture = new CompletableFuture<>();
        this.serverId = conf.getInt(ConfigOptions.COORDINATOR_ID);
    }

    public static void main(String[] args) {
        Configuration configuration =
                loadConfiguration(args, CoordinatorServer.class.getSimpleName());
        CoordinatorServer coordinatorServer = new CoordinatorServer(configuration);
        startServer(coordinatorServer);
    }

    @Override
    protected void startServices() throws Exception {
        electCoordinatorLeader();
    }

    private void electCoordinatorLeader() throws Exception {
        this.zkClient = ZooKeeperUtils.startZookeeperClient(conf, this);

        // Coordinator Server supports high availability. If 3 coordinator servers are alive,
        // one of them will be elected as leader and the other two will be standby.
        // When leader fails, one of standby coordinators will be elected as new leader.
        // All of them register to ZK like tablet servers in path
        // "/coordinators/ids/1","/coordinators/ids/2","/coordinators/ids/3".
        // but the leader will be elected in path "/coordinators/leader" additionally.
        registerCoordinatorServer();
        ZooKeeperUtils.registerZookeeperClientReInitSessionListener(
                zkClient, this::registerCoordinatorServer, this);

        // try to register Coordinator leader once
        if (tryElectCoordinatorLeaderOnce()) {
            startCoordinatorLeaderService();
        } else {
            // standby
            CoordinatorLeaderElection coordinatorLeaderElection =
                    new CoordinatorLeaderElection(zkClient.getCuratorClient(), serverId);
            coordinatorLeaderElection.startElectLeader(
                    () -> {
                        try {
                            startCoordinatorLeaderService();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    protected void startCoordinatorLeaderService() throws Exception {

        synchronized (lock) {
            LOG.info("Initializing Coordinator services.");
            List<Endpoint> endpoints = Endpoint.loadBindEndpoints(conf, ServerType.COORDINATOR);

            // for metrics
            this.metricRegistry = MetricRegistry.create(conf, pluginManager);
            this.serverMetricGroup =
                    ServerMetricUtils.createCoordinatorGroup(
                            metricRegistry,
                            ServerMetricUtils.validateAndGetClusterId(conf),
                            endpoints.get(0).getHost(),
                            serverId);

            this.coordinatorContext = new CoordinatorContext();
            this.metadataCache = new CoordinatorMetadataCache();

            this.authorizer = AuthorizerLoader.createAuthorizer(conf, zkClient, pluginManager);
            if (authorizer != null) {
                authorizer.startup();
            }

            this.lakeTableTieringManager = new LakeTableTieringManager();

            MetadataManager metadataManager = new MetadataManager(zkClient, conf);
            this.coordinatorService =
                    new CoordinatorService(
                            conf,
                            remoteFileSystem,
                            zkClient,
                            this::getCoordinatorEventProcessor,
                            metadataCache,
                            metadataManager,
                            authorizer,
                            createLakeCatalog(),
                            lakeTableTieringManager);

            this.rpcServer =
                    RpcServer.create(
                            conf,
                            endpoints,
                            coordinatorService,
                            serverMetricGroup,
                            RequestsMetrics.createCoordinatorServerRequestMetrics(
                                    serverMetricGroup));
            rpcServer.start();

            registerCoordinatorLeader();
            // when init session, register coordinator server again
            ZooKeeperUtils.registerZookeeperClientReInitSessionListener(
                    zkClient, this::registerCoordinatorLeader, this);

            this.clientMetricGroup = new ClientMetricGroup(metricRegistry, SERVER_NAME);
            this.rpcClient = RpcClient.create(conf, clientMetricGroup, true);

            this.coordinatorChannelManager = new CoordinatorChannelManager(rpcClient);

            this.autoPartitionManager =
                    new AutoPartitionManager(metadataCache, metadataManager, conf);
            autoPartitionManager.start();

            int ioExecutorPoolSize = conf.get(ConfigOptions.COORDINATOR_IO_POOL_SIZE);
            this.ioExecutor =
                    Executors.newFixedThreadPool(
                            ioExecutorPoolSize, new ExecutorThreadFactory("coordinator-io"));

            // start coordinator event processor after we register coordinator leader to zk
            // so that the event processor can get the coordinator leader node from zk during start
            // up.
            // in HA for coordinator server, the processor also need to know the leader node during
            // start up
            this.coordinatorEventProcessor =
                    new CoordinatorEventProcessor(
                            zkClient,
                            metadataCache,
                            coordinatorChannelManager,
                            coordinatorContext,
                            autoPartitionManager,
                            lakeTableTieringManager,
                            serverMetricGroup,
                            conf,
                            ioExecutor);
            coordinatorEventProcessor.startup();

            createDefaultDatabase();
        }
    }

    @Nullable
    private LakeCatalog createLakeCatalog() {
        DataLakeFormat dataLakeFormat = conf.get(ConfigOptions.DATALAKE_FORMAT);
        if (dataLakeFormat == null) {
            return null;
        }
        LakeStoragePlugin lakeStoragePlugin =
                LakeStoragePluginSetUp.fromDataLakeFormat(dataLakeFormat.toString(), pluginManager);
        Map<String, String> lakeProperties = extractLakeProperties(conf);
        LakeStorage lakeStorage =
                lakeStoragePlugin.createLakeStorage(
                        Configuration.fromMap(checkNotNull(lakeProperties)));
        return lakeStorage.createLakeCatalog();
    }

    @Override
    protected CompletableFuture<Result> closeAsync(Result result) {
        if (isShutDown.compareAndSet(false, true)) {
            LOG.info("Shutting down Coordinator server ({}).", result);
            CompletableFuture<Void> serviceShutdownFuture = stopServices();

            serviceShutdownFuture.whenComplete(
                    ((Void ignored2, Throwable serviceThrowable) -> {
                        if (serviceThrowable != null) {
                            terminationFuture.completeExceptionally(serviceThrowable);
                        } else {
                            terminationFuture.complete(result);
                        }
                    }));
        }

        return terminationFuture;
    }

    private void registerCoordinatorServer() throws Exception {
        long startTime = System.currentTimeMillis();

        // we need to retry to register since although
        // zkClient reconnect, the ephemeral node may still exist
        // for a while time, retry to wait the ephemeral node removed
        // see ZOOKEEPER-2985
        while (true) {
            try {
                zkClient.registerCoordinatorServer(this.serverId);
                break;
            } catch (KeeperException.NodeExistsException nodeExistsException) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                if (elapsedTime >= ZOOKEEPER_REGISTER_TOTAL_WAIT_TIME_MS) {
                    LOG.error(
                            "Coordinator Server register to Zookeeper exceeded total retry time of {} ms. "
                                    + "Aborting registration attempts.",
                            ZOOKEEPER_REGISTER_TOTAL_WAIT_TIME_MS);
                    throw nodeExistsException;
                }

                LOG.warn(
                        "Coordinator server already registered in Zookeeper. "
                                + "retrying register after {} ms....",
                        ZOOKEEPER_REGISTER_RETRY_INTERVAL_MS);
                try {
                    Thread.sleep(ZOOKEEPER_REGISTER_RETRY_INTERVAL_MS);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private boolean tryElectCoordinatorLeaderOnce() throws Exception {
        try {
            zkClient.electCoordinatorLeader();
            LOG.info("Coordinator server {} win the leader in election now.", serverId);
            return true;
        } catch (KeeperException.NodeExistsException nodeExistsException) {
            LOG.warn(
                    "Coordinator leader already registered in Zookeeper. Coordinator {} will be standby",
                    serverId);
            return false;
        }
    }

    private void registerCoordinatorLeader() throws Exception {
        long startTime = System.currentTimeMillis();
        List<Endpoint> bindEndpoints = rpcServer.getBindEndpoints();
        CoordinatorAddress coordinatorAddress =
                new CoordinatorAddress(
                        this.serverId, Endpoint.loadAdvertisedEndpoints(bindEndpoints, conf));

        // we need to retry to register since although
        // zkClient reconnect, the ephemeral node may still exist
        // for a while time, retry to wait the ephemeral node removed
        // see ZOOKEEPER-2985
        while (true) {
            try {
                zkClient.registerCoordinatorLeader(coordinatorAddress);
                break;
            } catch (KeeperException.NodeExistsException nodeExistsException) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                if (elapsedTime >= ZOOKEEPER_REGISTER_TOTAL_WAIT_TIME_MS) {
                    LOG.error(
                            "Coordinator Server register to Zookeeper exceeded total retry time of {} ms. "
                                    + "Aborting registration attempts.",
                            ZOOKEEPER_REGISTER_TOTAL_WAIT_TIME_MS);
                    throw nodeExistsException;
                }

                LOG.warn(
                        "Coordinator server already registered in Zookeeper. "
                                + "retrying register after {} ms....",
                        ZOOKEEPER_REGISTER_RETRY_INTERVAL_MS);
                try {
                    Thread.sleep(ZOOKEEPER_REGISTER_RETRY_INTERVAL_MS);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void createDefaultDatabase() {
        MetadataManager metadataManager = new MetadataManager(zkClient, conf);
        List<String> databases = metadataManager.listDatabases();
        if (databases.isEmpty()) {
            metadataManager.createDatabase(DEFAULT_DATABASE, DatabaseDescriptor.EMPTY, true);
            LOG.info("Created default database '{}' because no database exists.", DEFAULT_DATABASE);
        }
        // create Kafka default database if Kafka is enabled.
        if (conf.get(ConfigOptions.KAFKA_ENABLED)) {
            String kafkaDB = conf.get(ConfigOptions.KAFKA_DATABASE);
            if (!databases.contains(kafkaDB)) {
                metadataManager.createDatabase(kafkaDB, DatabaseDescriptor.EMPTY, true);
                LOG.info("Created default database '{}' for Kafka protocol.", kafkaDB);
            }
        }
    }

    private CoordinatorEventProcessor getCoordinatorEventProcessor() {
        if (coordinatorEventProcessor != null) {
            return coordinatorEventProcessor;
        } else {
            throw new IllegalStateException("CoordinatorEventProcessor is not initialized yet.");
        }
    }

    CompletableFuture<Void> stopServices() {
        synchronized (lock) {
            Throwable exception = null;

            try {
                if (serverMetricGroup != null) {
                    serverMetricGroup.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(2);
            try {
                if (metricRegistry != null) {
                    terminationFutures.add(metricRegistry.closeAsync());
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (autoPartitionManager != null) {
                    autoPartitionManager.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (ioExecutor != null) {
                    // shutdown io executor
                    ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, ioExecutor);
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (coordinatorEventProcessor != null) {
                    coordinatorEventProcessor.shutdown();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (coordinatorChannelManager != null) {
                    coordinatorChannelManager.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (rpcServer != null) {
                    terminationFutures.add(rpcServer.closeAsync());
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (coordinatorService != null) {
                    coordinatorService.shutdown();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (coordinatorContext != null) {
                    // then reset coordinatorContext
                    coordinatorContext.resetContext();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (lakeTableTieringManager != null) {
                    lakeTableTieringManager.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (zkClient != null) {
                    zkClient.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (authorizer != null) {
                    authorizer.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (rpcClient != null) {
                    rpcClient.close();
                }

                if (clientMetricGroup != null) {
                    clientMetricGroup.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            if (exception != null) {
                terminationFutures.add(FutureUtils.completedExceptionally(exception));
            }
            return FutureUtils.completeAll(terminationFutures);
        }
    }

    @Override
    protected CompletableFuture<Result> getTerminationFuture() {
        return terminationFuture;
    }

    @VisibleForTesting
    public CoordinatorService getCoordinatorService() {
        return coordinatorService;
    }

    @Override
    protected String getServerName() {
        return SERVER_NAME;
    }

    @VisibleForTesting
    public RpcServer getRpcServer() {
        return rpcServer;
    }

    @VisibleForTesting
    public int getServerId() {
        return serverId;
    }

    @VisibleForTesting
    public ServerMetadataCache getMetadataCache() {
        return metadataCache;
    }

    @VisibleForTesting
    public @Nullable Authorizer getAuthorizer() {
        return authorizer;
    }

    private static void validateConfigs(Configuration conf) {
        Optional<Integer> serverId = conf.getOptional(ConfigOptions.COORDINATOR_ID);
        if (!serverId.isPresent()) {
            throw new IllegalConfigurationException(
                    String.format("Configuration %s must be set.", ConfigOptions.COORDINATOR_ID));
        }

        if (serverId.get() < 0) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for %s, it must be greater than or equal 0.",
                            ConfigOptions.COORDINATOR_ID.key()));
        }

        if (conf.get(ConfigOptions.DEFAULT_REPLICATION_FACTOR) < 1) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for %s, it must be greater than or equal 1.",
                            ConfigOptions.DEFAULT_REPLICATION_FACTOR.key()));
        }
        if (conf.get(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS) < 1) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for %s, it must be greater than or equal 1.",
                            ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS.key()));
        }

        if (conf.get(ConfigOptions.COORDINATOR_IO_POOL_SIZE) < 1) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for %s, it must be greater than or equal 1.",
                            ConfigOptions.COORDINATOR_IO_POOL_SIZE.key()));
        }

        if (conf.get(ConfigOptions.REMOTE_DATA_DIR) == null) {
            throw new IllegalConfigurationException(
                    String.format("Configuration %s must be set.", ConfigOptions.REMOTE_DATA_DIR));
        }
    }
}
