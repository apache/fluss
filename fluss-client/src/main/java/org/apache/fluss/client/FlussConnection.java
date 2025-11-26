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

package org.apache.fluss.client;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.lookup.LookupClient;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.FlussTable;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.client.token.DefaultSecurityTokenManager;
import org.apache.fluss.client.token.DefaultSecurityTokenProvider;
import org.apache.fluss.client.token.SecurityTokenManager;
import org.apache.fluss.client.token.SecurityTokenProvider;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;

import static org.apache.fluss.client.utils.MetadataUtils.getOneAvailableTabletServerNode;
import static org.apache.fluss.config.FlussConfigUtils.CLIENT_PREFIX;
import static org.apache.fluss.utils.PropertiesUtils.extractPrefix;

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
    private volatile Admin admin;

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
        this.rpcClient = RpcClient.create(conf, clientMetricGroup, false);

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
        return getOrCreateAdmin();
    }

    @Override
    public Table getTable(TablePath tablePath) {
        // force to update the table info from server to avoid stale data in cache.
        metadataUpdater.updateTableOrPartitionMetadata(tablePath, null);
        Admin admin = getOrCreateAdmin();
        return new FlussTable(this, tablePath, admin.getTableInfo(tablePath).join());
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

    public Admin getOrCreateAdmin() {
        if (admin == null) {
            synchronized (this) {
                if (admin == null) {
                    admin = new FlussAdmin(rpcClient, metadataUpdater);
                }
            }
        }
        return admin;
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
