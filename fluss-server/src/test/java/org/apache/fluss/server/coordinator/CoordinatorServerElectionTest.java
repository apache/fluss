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
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.rpc.messages.AlterClusterConfigsRequest;
import org.apache.fluss.rpc.messages.PbAlterConfig;
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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        CoordinatorServer coordinatorServer1 = new CoordinatorServer(createConfiguration());
        CoordinatorServer coordinatorServer2 = new CoordinatorServer(createConfiguration());
        CoordinatorServer coordinatorServer3 = new CoordinatorServer(createConfiguration());

        List<CoordinatorServer> coordinatorServerList =
                Arrays.asList(coordinatorServer1, coordinatorServer2, coordinatorServer3);

        // start 3 coordinator servers
        for (int i = 0; i < 3; i++) {
            CoordinatorServer server = coordinatorServerList.get(i);
            server.start();
        }

        // random coordinator become leader
        waitUntilCoordinatorServerElected();

        CoordinatorAddress firstLeaderAddress = zookeeperClient.getCoordinatorLeaderAddress().get();

        // Find the leader and try to restart it.
        CoordinatorServer firstLeader = null;
        for (CoordinatorServer coordinatorServer : coordinatorServerList) {
            if (Objects.equals(coordinatorServer.getServerId(), firstLeaderAddress.getId())) {
                firstLeader = coordinatorServer;
                break;
            }
        }
        assertThat(firstLeader).isNotNull();
        assertThat(zookeeperClient.getCurrentEpoch().getCoordinatorEpoch())
                .isEqualTo(CoordinatorContext.INITIAL_COORDINATOR_EPOCH);
        firstLeader
                .getCoordinatorService()
                .alterClusterConfigs(manifestV2WriterConfig("true"))
                .get(10, TimeUnit.SECONDS);
        waitUntil(
                () ->
                        coordinatorServerList.stream()
                                .allMatch(
                                        server ->
                                                server
                                                        .getDynamicConfigManager()
                                                        .describeConfigs()
                                                        .stream()
                                                        .anyMatch(
                                                                entry ->
                                                                        entry.key()
                                                                                        .equals(
                                                                                                ConfigOptions
                                                                                                        .REMOTE_LOG_MANIFEST_V2_WRITER_ENABLED
                                                                                                        .key())
                                                                                && entry.value()
                                                                                        .equals(
                                                                                                "true"))),
                Duration.ofSeconds(10),
                "Manifest V2 writer config did not reach all coordinators");
        firstLeader.close();
        firstLeader.start();

        // Then we should get another Coordinator server leader elected
        waitUntilCoordinatorServerReelected(firstLeaderAddress);
        CoordinatorAddress secondLeaderAddress =
                zookeeperClient.getCoordinatorLeaderAddress().get();
        assertThat(secondLeaderAddress).isNotEqualTo(firstLeaderAddress);
        assertThat(zookeeperClient.getCurrentEpoch().getCoordinatorEpoch())
                .isEqualTo(CoordinatorContext.INITIAL_COORDINATOR_EPOCH + 1);

        CoordinatorServer secondLeader = null;
        for (CoordinatorServer coordinatorServer : coordinatorServerList) {
            if (Objects.equals(coordinatorServer.getServerId(), secondLeaderAddress.getId())) {
                secondLeader = coordinatorServer;
                break;
            }
        }
        assertThat(secondLeader).isNotNull();
        CoordinatorServer promotedLeader = secondLeader;
        assertThatThrownBy(
                        () ->
                                promotedLeader
                                        .getCoordinatorService()
                                        .alterClusterConfigs(manifestV2WriterConfig("false"))
                                        .get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ConfigException.class)
                .hasMessageContaining("activation is irreversible");
        CoordinatorServer nonLeader = null;
        for (CoordinatorServer coordinatorServer : coordinatorServerList) {
            if (!Objects.equals(coordinatorServer.getServerId(), firstLeaderAddress.getId())
                    && !Objects.equals(
                            coordinatorServer.getServerId(), secondLeaderAddress.getId())) {
                nonLeader = coordinatorServer;
                break;
            }
        }
        // kill other 2 coordinator servers except the first one
        nonLeader.close();
        secondLeader.close();

        // the origin coordinator server should become leader again
        waitUntilCoordinatorServerReelected(secondLeaderAddress);
        CoordinatorAddress thirdLeaderAddress = zookeeperClient.getCoordinatorLeaderAddress().get();

        assertThat(thirdLeaderAddress.getId()).isEqualTo(firstLeaderAddress.getId());
        assertThat(zookeeperClient.getCurrentEpoch().getCoordinatorEpoch())
                .isEqualTo(CoordinatorContext.INITIAL_COORDINATOR_EPOCH + 2);
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

    private static AlterClusterConfigsRequest manifestV2WriterConfig(String value) {
        PbAlterConfig config =
                new PbAlterConfig()
                        .setConfigKey(ConfigOptions.REMOTE_LOG_MANIFEST_V2_WRITER_ENABLED.key())
                        .setConfigValue(value)
                        .setOpType(AlterConfigOpType.SET.value());
        return new AlterClusterConfigsRequest()
                .addAllAlterConfigs(Collections.singletonList(config));
    }

    public void waitUntilCoordinatorServerElected() {
        waitUntil(
                () -> zookeeperClient.getCoordinatorLeaderAddress().isPresent(),
                Duration.ofMinutes(1),
                "Fail to wait coordinator server elected");
    }

    public void waitUntilCoordinatorServerReelected(CoordinatorAddress originAddress) {
        waitUntil(
                () -> {
                    Optional<CoordinatorAddress> address =
                            zookeeperClient.getCoordinatorLeaderAddress();
                    return address.isPresent() && !address.get().equals(originAddress);
                },
                Duration.ofMinutes(1),
                "Fail to wait coordinator server reelected");
    }
}
