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

package org.apache.fluss.server.tablet;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.server.ServerBase;
import org.apache.fluss.server.ServerTestBase;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.ServerProtocolVersion;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.listen.StandardListenerManager;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.state.ConnectionState;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TabletServer}. */
class TabletServerTest extends ServerTestBase {

    private static final int SERVER_ID = 0;
    private static final String RACK = "cn-hangzhou-server10";

    private static @TempDir File tempDirForLog;

    private TabletServer server;

    @BeforeEach
    void before() throws Exception {
        Configuration conf = createTabletServerConfiguration();
        server = new TabletServer(conf);
        server.start();
    }

    @AfterEach
    void after() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = ConnectionState.class,
            names = {"SUSPENDED", "LOST"})
    void testConnectionChangeDuringInitialRegistrationRejectsStartup(ConnectionState state)
            throws Exception {
        server.close();
        Configuration configuration = createTabletServerConfiguration();
        server =
                new TabletServer(configuration) {
                    @Override
                    ZooKeeperClient createZooKeeperClient() {
                        StandardListenerManager<ConnectionStateListener> listeners =
                                StandardListenerManager.standard();
                        CuratorFrameworkWithUnhandledErrorListener wrapper =
                                ZooKeeperUtils.startZookeeperClient(
                                        CuratorFrameworkFactory.builder()
                                                .connectString(
                                                        configuration.getString(
                                                                ConfigOptions.ZOOKEEPER_ADDRESS))
                                                .namespace(
                                                        ZooKeeperUtils.trimStartingSlash(
                                                                configuration.getString(
                                                                        ConfigOptions
                                                                                .ZOOKEEPER_ROOT)))
                                                .retryPolicy(new ExponentialBackoffRetry(100, 3))
                                                .connectionStateListenerManagerFactory(
                                                        ignored -> listeners),
                                        this);
                        return new ZooKeeperClient(wrapper, configuration) {
                            @Override
                            public void registerTabletServer(
                                    int serverId, TabletServerRegistration registration)
                                    throws Exception {
                                super.registerTabletServer(serverId, registration);
                                listeners.forEach(
                                        listener ->
                                                listener.stateChanged(getCuratorClient(), state));
                            }
                        };
                    }
                };

        assertThatThrownBy(server::start)
                .isInstanceOf(FlussException.class)
                .hasRootCauseInstanceOf(StaleMetadataException.class);
    }

    @Override
    protected ServerBase getServer() {
        return server;
    }

    @Override
    protected ServerBase getStartFailServer() {
        Configuration configuration = createTabletServerConfiguration();
        // configure with a invalid port, the server should fail to start
        configuration.set(ConfigOptions.BIND_LISTENERS, "FLUSS://localhost:-12");
        return new TabletServer(configuration);
    }

    private static Configuration createTabletServerConfiguration() {
        Configuration configuration = createConfiguration();
        configuration.set(ConfigOptions.TABLET_SERVER_ID, SERVER_ID);
        configuration.set(ConfigOptions.TABLET_SERVER_RACK, RACK);
        configuration.setString(ConfigOptions.DATA_DIR, tempDirForLog.getAbsolutePath());
        return configuration;
    }

    @Override
    protected void checkAfterStartServer() throws Exception {
        // check the data put in zk after tablet server start
        Optional<TabletServerRegistration> optionalTabletServerRegistration =
                zookeeperClient.getTabletServer(SERVER_ID);
        assertThat(optionalTabletServerRegistration).isPresent();

        TabletServerRegistration tabletServerRegistration = optionalTabletServerRegistration.get();
        assertThat(tabletServerRegistration.getRack()).isEqualTo(RACK);
        verifyEndpoint(
                tabletServerRegistration.getEndpoints(), server.getRpcServer().getBindEndpoints());
        assertThat(tabletServerRegistration.getServerProtocolVersion())
                .isEqualTo(ServerProtocolVersion.CURRENT);
    }
}
