/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

class CoordinatorServerElectionTest {
    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    protected static ZooKeeperClient zookeeperClient;

    @BeforeAll
    static void baseBeforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @Test
    void testCoordinatorServerElection() throws Exception {
        CoordinatorServer coordinatorServer1 = new CoordinatorServer(createConfiguration(1));
        CoordinatorServer coordinatorServer2 = new CoordinatorServer(createConfiguration(2));
        CoordinatorServer coordinatorServer3 = new CoordinatorServer(createConfiguration(3));

        List<CoordinatorServer> coordinatorServerList =
                Arrays.asList(coordinatorServer1, coordinatorServer2, coordinatorServer3);

        ExecutorService executor = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 3; i++) {
            CoordinatorServer server = coordinatorServerList.get(i);
            executor.submit(
                    () -> {
                        try {
                            server.start();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
        }

        waitUntilCoordinatorServerElected();

        CoordinatorAddress firstLeaderAddress = zookeeperClient.getCoordinatorLeaderAddress().get();

        // Find the Coordinator server leader
        // and try to close it.
        // Then we should get another Coordinator server leader elected
        CoordinatorServer elected = null;
        for (CoordinatorServer coordinatorServer : coordinatorServerList) {
            if (coordinatorServer.getServerId() == firstLeaderAddress.getId()) {
                elected = coordinatorServer;
                break;
            }
        }
        assertThat(elected).isNotNull();
        elected.close();

        // coordinator leader changed.
        waitUntilCoordinatorServerReelected(firstLeaderAddress);
        CoordinatorAddress secondLeaderAddress =
                zookeeperClient.getCoordinatorLeaderAddress().get();
        assertThat(secondLeaderAddress).isNotEqualTo(firstLeaderAddress);
    }

    /** Create a configuration with Zookeeper address setting. */
    protected static Configuration createConfiguration(int serverId) {
        Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        configuration.setString(
                ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,FLUSS://localhost:0");
        configuration.setString(ConfigOptions.ADVERTISED_LISTENERS, "CLIENT://198.168.0.1:100");
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");

        // set to small timout to verify the case that zk session is timeout
        configuration.set(ConfigOptions.ZOOKEEPER_SESSION_TIMEOUT, Duration.ofMillis(500));
        configuration.set(ConfigOptions.ZOOKEEPER_CONNECTION_TIMEOUT, Duration.ofMillis(500));
        configuration.set(ConfigOptions.ZOOKEEPER_RETRY_WAIT, Duration.ofMillis(500));

        configuration.set(ConfigOptions.COORDINATOR_ID, serverId);
        return configuration;
    }

    public void waitUntilCoordinatorServerElected() {
        waitUntil(
                () -> {
                    return zookeeperClient.getCoordinatorLeaderAddress().isPresent();
                },
                Duration.ofMinutes(1),
                "Fail to wait coordinator server elected");
    }

    public void waitUntilCoordinatorServerReelected(CoordinatorAddress address) {
        waitUntil(
                () -> {
                    return zookeeperClient.getCoordinatorLeaderAddress().isPresent()
                            && !zookeeperClient.getCoordinatorLeaderAddress().get().equals(address);
                },
                Duration.ofMinutes(1),
                String.format("Fail to wait coordinator server reelected"));
    }
}
