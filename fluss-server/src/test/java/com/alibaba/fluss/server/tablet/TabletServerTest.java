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
import com.alibaba.fluss.utils.NetUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TabletServer}. */
class TabletServerTest extends ServerTestBase {

    private static final int SERVER_ID = 0;

    private static @TempDir File tempDirForLog;

    private TabletServer server;
    private int port;

    @BeforeEach
    void before() throws Exception {
        try (NetUtils.Port p = NetUtils.getAvailablePort()) {
            port = p.getPort();
            Configuration conf = createTabletServerConfiguration();
            conf.set(ConfigOptions.TABLET_SERVER_PORT, port + "");
            server = new TabletServer(conf);
            server.start();
        }
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
        configuration.setString(ConfigOptions.TABLET_SERVER_PORT, "-12");
        return new TabletServer(configuration);
    }

    private static Configuration createTabletServerConfiguration() {
        Configuration configuration = createConfiguration();
        configuration.set(ConfigOptions.TABLET_SERVER_ID, SERVER_ID);
        configuration.set(ConfigOptions.TABLET_SERVER_HOST, "localhost");
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
        assertThat(tabletServerRegistration.getHost()).isEqualTo("127.0.0.1");
        assertThat(tabletServerRegistration.getPort()).isEqualTo(port);
    }
}
