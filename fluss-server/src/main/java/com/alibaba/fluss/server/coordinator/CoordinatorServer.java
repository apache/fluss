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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.IllegalConfigurationException;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.RpcServer;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.rpc.netty.server.RequestsMetrics;
import com.alibaba.fluss.server.ServerBase;
import com.alibaba.fluss.server.coordinator.event.CoordinatorEventManager;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.metadata.ServerMetadataCacheImpl;
import com.alibaba.fluss.server.metrics.ServerMetricUtils;
import com.alibaba.fluss.server.metrics.group.CoordinatorMetricGroup;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperUtils;
import com.alibaba.fluss.server.zk.data.CoordinatorAddress;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.ExecutorUtils;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;
import com.alibaba.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private String serverId;

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
    private ServerMetadataCache metadataCache;

    @GuardedBy("lock")
    private CoordinatorChannelManager coordinatorChannelManager;

    @GuardedBy("lock")
    private CoordinatorEventProcessor coordinatorEventProcessor;

    @GuardedBy("lock")
    private ZooKeeperClient zkClient;

    @GuardedBy("lock")
    private AutoPartitionManager autoPartitionManager;

    @GuardedBy("lock")
    private ExecutorService ioExecutor;

    public CoordinatorServer(Configuration conf) {
        super(conf);
        validateConfigs(conf);
        this.terminationFuture = new CompletableFuture<>();
    }

    public static void main(String[] args) {
        Configuration configuration =
                loadConfiguration(args, CoordinatorServer.class.getSimpleName());
        CoordinatorServer coordinatorServer = new CoordinatorServer(configuration);
        startServer(coordinatorServer);
    }

    @Override
    protected void startServices() throws Exception {
        synchronized (lock) {
            LOG.info("Initializing Coordinator services.");
            List<Endpoint> endpoints = Endpoint.loadBindEndpoints(conf, ServerType.COORDINATOR);
            this.serverId = UUID.randomUUID().toString();

            // for metrics
            this.metricRegistry = MetricRegistry.create(conf, pluginManager);
            this.serverMetricGroup =
                    ServerMetricUtils.createCoordinatorGroup(
                            metricRegistry,
                            ServerMetricUtils.validateAndGetClusterId(conf),
                            endpoints.get(0).getHost(),
                            serverId);

            this.zkClient = ZooKeeperUtils.startZookeeperClient(conf, this);

            this.metadataCache = new ServerMetadataCacheImpl();

            MetadataManager metadataManager = new MetadataManager(zkClient, conf);
            this.coordinatorService =
                    new CoordinatorService(
                            conf,
                            remoteFileSystem,
                            zkClient,
                            this::getCoordinatorEventManager,
                            metadataCache,
                            metadataManager);

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

            this.clientMetricGroup = new ClientMetricGroup(metricRegistry, SERVER_NAME);
            this.rpcClient = RpcClient.create(conf, clientMetricGroup);

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
                            autoPartitionManager,
                            serverMetricGroup,
                            conf,
                            ioExecutor);
            coordinatorEventProcessor.startup();

            createDefaultDatabase();
        }
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

    private void registerCoordinatorLeader() throws Exception {
        List<Endpoint> bindEndpoints = rpcServer.getBindEndpoints();
        CoordinatorAddress coordinatorAddress =
                new CoordinatorAddress(
                        this.serverId, Endpoint.loadAdvertisedEndpoints(bindEndpoints, conf));
        zkClient.registerCoordinatorLeader(coordinatorAddress);
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

    private CoordinatorEventManager getCoordinatorEventManager() {
        if (coordinatorEventProcessor != null) {
            return coordinatorEventProcessor.getCoordinatorEventManager();
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
                if (zkClient != null) {
                    zkClient.close();
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

    private static void validateConfigs(Configuration conf) {
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
