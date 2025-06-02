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

package com.alibaba.fluss.server.tablet;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.server.ServerBase;
import com.alibaba.fluss.server.ServerTestBase;
import com.alibaba.fluss.server.zk.data.TabletServerRegistration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

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
    }
}
