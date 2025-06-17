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

package com.alibaba.fluss.client;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.admin.FlussAdmin;
import com.alibaba.fluss.client.lookup.LookupClient;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.table.FlussTable;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.RemoteFileDownloader;
import com.alibaba.fluss.client.token.DefaultSecurityTokenManager;
import com.alibaba.fluss.client.token.DefaultSecurityTokenProvider;
import com.alibaba.fluss.client.token.SecurityTokenManager;
import com.alibaba.fluss.client.token.SecurityTokenProvider;
import com.alibaba.fluss.client.write.WriterClient;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.AdminReadOnlyGateway;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;

import static com.alibaba.fluss.client.utils.MetadataUtils.getOneAvailableTabletServerNode;
import static com.alibaba.fluss.config.FlussConfigUtils.CLIENT_PREFIX;
import static com.alibaba.fluss.utils.PropertiesUtils.extractPrefix;

/** A connection to Fluss cluster, and holds the client session resources. */
public final class FlussConnection implements Connection {
    private final Configuration conf;
    private final RpcClient rpcClient;
    private final MetadataUpdater metadataUpdater;
    private final MetricRegistry metricRegistry;
    private final ClientMetricGroup clientMetricGroup;

    private volatile WriterClient writerClient;
    private volatile LookupClient lookupClient;
    private volatile RemoteFileDownloader remoteFileDownloader;
    private volatile SecurityTokenManager securityTokenManager;

    FlussConnection(Configuration conf) {
        this(conf, MetricRegistry.create(conf, null));
    }

    FlussConnection(Configuration conf, MetricRegistry metricRegistry) {
        this.conf = conf;
        // init Filesystem with configuration from FlussConnection,
        // only pass options with 'client.fs.' prefix
        FileSystem.initialize(
                Configuration.fromMap(
                        extractPrefix(new HashMap<>(conf.toMap()), CLIENT_PREFIX + "fs.")),
                null);
        // for client metrics.
        setupClientMetricsConfiguration();
        String clientId = conf.getString(ConfigOptions.CLIENT_ID);
        this.metricRegistry = metricRegistry;
        this.clientMetricGroup = new ClientMetricGroup(metricRegistry, clientId);
        this.rpcClient = RpcClient.create(conf, clientMetricGroup);

        // TODO this maybe remove after we introduce client metadata.
        this.metadataUpdater = new MetadataUpdater(conf, rpcClient);
        this.writerClient = null;
    }

    @Override
    public Configuration getConfiguration() {
        return conf;
    }

    @Override
    public Admin getAdmin() {
        return new FlussAdmin(rpcClient, metadataUpdater);
    }

    @Override
    public Table getTable(TablePath tablePath) {
        // force to update the table info from server to avoid stale data in cache
        metadataUpdater.updateTableOrPartitionMetadata(tablePath, null);
        TableInfo tableInfo = metadataUpdater.getTableInfoOrElseThrow(tablePath);
        return new FlussTable(this, tablePath, tableInfo);
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    public MetadataUpdater getMetadataUpdater() {
        return metadataUpdater;
    }

    public ClientMetricGroup getClientMetricGroup() {
        return clientMetricGroup;
    }

    public WriterClient getOrCreateWriterClient() {
        if (writerClient == null) {
            synchronized (this) {
                if (writerClient == null) {
                    writerClient =
                            new WriterClient(
                                    conf, metadataUpdater, clientMetricGroup, this.getAdmin());
                }
            }
        }
        return writerClient;
    }

    public LookupClient getOrCreateLookupClient() {
        if (lookupClient == null) {
            synchronized (this) {
                if (lookupClient == null) {
                    lookupClient = new LookupClient(conf, metadataUpdater);
                }
            }
        }
        return lookupClient;
    }

    public RemoteFileDownloader getOrCreateRemoteFileDownloader() {
        if (remoteFileDownloader == null) {
            synchronized (this) {
                if (remoteFileDownloader == null) {
                    remoteFileDownloader =
                            new RemoteFileDownloader(
                                    conf.getInt(ConfigOptions.REMOTE_FILE_DOWNLOAD_THREAD_NUM));
                }
                // access remote files requires setting up filesystem security token manager
                if (securityTokenManager == null) {
                    // prepare security token manager
                    // create the admin read only gateway
                    // todo: may add retry logic when no any available tablet server?
                    AdminReadOnlyGateway gateway =
                            GatewayClientProxy.createGatewayProxy(
                                    () ->
                                            getOneAvailableTabletServerNode(
                                                    metadataUpdater.getCluster()),
                                    rpcClient,
                                    AdminReadOnlyGateway.class);
                    SecurityTokenProvider securityTokenProvider =
                            new DefaultSecurityTokenProvider(gateway);
                    securityTokenManager =
                            new DefaultSecurityTokenManager(conf, securityTokenProvider);
                    try {
                        securityTokenManager.start();
                    } catch (Exception e) {
                        throw new FlussRuntimeException("start security token manager failed", e);
                    }
                }
            }
        }
        return remoteFileDownloader;
    }

    @Override
    public void close() throws Exception {
        if (writerClient != null) {
            writerClient.close(Duration.ofMillis(Long.MAX_VALUE));
        }

        if (lookupClient != null) {
            // timeout is Long.MAX_VALUE to make the pending get request
            // to be processed
            lookupClient.close(Duration.ofMillis(Long.MAX_VALUE));
        }

        if (remoteFileDownloader != null) {
            remoteFileDownloader.close();
        }

        if (securityTokenManager != null) {
            // todo: FLUSS-56910234 we don't have to wait until close fluss table
            // to stop securityTokenManager
            securityTokenManager.stop();
        }

        clientMetricGroup.close();
        rpcClient.close();
        metricRegistry.closeAsync().get();
    }

    private void setupClientMetricsConfiguration() {
        boolean enableClientMetrics = conf.getBoolean(ConfigOptions.CLIENT_METRICS_ENABLED);
        List<String> reporters = conf.get(ConfigOptions.METRICS_REPORTERS);
        if (enableClientMetrics && (reporters == null || reporters.isEmpty())) {
            // Client will use JMX reporter by default if not set.
            conf.setString(ConfigOptions.METRICS_REPORTERS.key(), "jmx");
        }
    }
}
