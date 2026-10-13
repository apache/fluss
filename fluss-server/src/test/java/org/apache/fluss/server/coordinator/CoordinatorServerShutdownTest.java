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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;

/** Test for shutting down {@link CoordinatorServer}. */
class CoordinatorServerShutdownTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zooKeeperClient;

    @BeforeAll
    static void beforeAll() {
        zooKeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @Test
    void testCloseCleansUpCoordinatorLeader() throws Exception {
        CoordinatorServer coordinatorServer = new CoordinatorServer(createConfiguration());
        coordinatorServer.start();
        waitUntil(
                () -> zooKeeperClient.getCoordinatorLeaderAddress().isPresent(),
                Duration.ofSeconds(30),
                "Coordinator server was not elected");

        CompletableFuture<Void> closeFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                coordinatorServer.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        closeFuture.get(30, TimeUnit.SECONDS);

        waitUntil(
                () -> !zooKeeperClient.getCoordinatorLeaderAddress().isPresent(),
                Duration.ofSeconds(30),
                "Coordinator leader was not removed after shutdown");
    }

    private static Configuration createConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        configuration.setString(
                ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,FLUSS://localhost:0");
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");
        return configuration;
    }
}
