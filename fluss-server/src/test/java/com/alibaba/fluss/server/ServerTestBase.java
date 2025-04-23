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

package com.alibaba.fluss.server;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussException;
import com.alibaba.fluss.server.coordinator.CoordinatorServer;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A base test for Server (coordinator & tablet server). */
public abstract class ServerTestBase {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    protected static ZooKeeperClient zookeeperClient;

    protected abstract ServerBase getServer();

    protected abstract ServerBase getStartFailServer();

    protected abstract void checkAfterStartServer() throws Exception;

    @BeforeAll
    static void baseBeforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @Test
    void testStartServer() throws Exception {
        // check logic after start the server
        checkAfterStartServer();
    }

    @Test
    void testShouldShutdownOnFatalError() {
        ServerBase server = getServer();

        // on fatal error
        server.onFatalError(new RuntimeException());
        assertThat(server.getTerminationFuture().join()).isEqualTo(ServerBase.Result.FAILURE);
    }

    @Test
    void testExceptionWhenRunServer() throws Exception {
        ServerBase server = getStartFailServer();
        assertThatThrownBy(server::start)
                .isInstanceOf(FlussException.class)
                .hasMessage(String.format("Failed to start the %s.", server.getServerName()));
        server.close();
    }

    /** Create a configuration with Zookeeper address setting. */
    protected static Configuration createConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        configuration.setString(
                ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,FLUSS://localhost:0");
        configuration.setString(ConfigOptions.ADVERTISED_LISTENERS, "CLIENT://198.168.0.1:100");
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");
        return configuration;
    }

    protected void verifyEndpoint(
            List<Endpoint> registeredEndpoints, List<Endpoint> bindEndpoints) {
        Endpoint internal =
                bindEndpoints.stream()
                        .filter(e -> e.getListenerName().equals("FLUSS"))
                        .findFirst()
                        .get();
        List<Endpoint> expectedEndpoints =
                Endpoint.fromListenersString(
                        internal.listenerString() + ", CLIENT://198.168.0.1:100");
        assertThat(registeredEndpoints).containsExactlyInAnyOrderElementsOf(expectedEndpoints);
    }

    public static CoordinatorServer startCoordinatorServer(Configuration conf) throws Exception {
        CoordinatorServer coordinatorServer = new CoordinatorServer(conf);
        coordinatorServer.start();
        return coordinatorServer;
    }
}
