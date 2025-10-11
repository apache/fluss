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

package org.apache.fluss.compatibilitytest;

import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.RetryOneTime;

import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.INT_TYPE;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.STRING_TYPE;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Basic abstract class for fluss server upgrade compatibility test. */
@Testcontainers
public abstract class ServerUpgradeCompatTest extends CompatTest {
    protected void serverUpgradeTestPipeline(
            int clientVersionMagic, int lowerServerVersionMagic, int higherServerVersionMagic)
            throws Exception {
        // start the lower server.
        initAndStartFlussServer(clientVersionMagic, lowerServerVersionMagic);
        initFlussConnection(lowerServerVersionMagic);
        initFlussAdmin();

        // create table and insert data.
        String dbName = "upgrade-test-db-1";
        String tableName = "upgrade-test-table-1";
        createTableAndInsertData(dbName, tableName);

        // stop the lower server.
        stopServer();

        // verify zookeeper and fs to check the data.

        // Starting from Fluss-0.8, Fluss supports actively retrying when TabletServer and
        // CoordinatorServer have left registration meta in ZooKeeper, to prevent the servers from
        // failing to start. To avoid startup errors, we proactively clean up any leftover server
        // registration meta in ZooKeeper here.
        cleanServerMetaFromZk();

        // start the higher server.
        initAndStartFlussServer(clientVersionMagic, higherServerVersionMagic);
        initFlussConnection(higherServerVersionMagic);
        initFlussAdmin();

        // get table and get data.
        getTableAndGetData(dbName, tableName);

        // stop the higher server.
        stopServer();
    }

    private void createTableAndInsertData(String dbName, String tableName) throws Exception {
        createDatabase(dbName);
        createTable(
                new TestingTableDescriptor(
                        dbName,
                        tableName,
                        Arrays.asList("a", "b", "c"),
                        Arrays.asList(INT_TYPE, STRING_TYPE, STRING_TYPE),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap()));
        assertThat(tableExists(dbName, tableName)).isTrue();

        // TODO insert data.
    }

    private void getTableAndGetData(String dbName, String tableName) throws Exception {
        assertThat(tableExists(dbName, tableName)).isTrue();

        // TODO get data.
    }

    private void cleanServerMetaFromZk() {
        String coordinatorPath = "/fluss/coordinators";
        String coordinatorActivePath = coordinatorPath + "/active";
        String tabletServerPath = "/fluss/tabletservers";
        String idsPath = tabletServerPath + "/ids";

        String connectionString = ZOOKEEPER.getHost() + ":" + ZOOKEEPER.getMappedPort(2181);
        CuratorFramework client =
                CuratorFrameworkFactory.builder()
                        .connectString(connectionString)
                        .retryPolicy(new RetryOneTime(100))
                        .build();
        client.start();

        // wait coordinatorServer meta removed from ZooKeeper.
        waitUntil(
                () -> {
                    if (client.checkExists().forPath(coordinatorPath) != null) {
                        if (client.checkExists().forPath(coordinatorActivePath) != null) {
                            try {
                                client.delete().forPath(coordinatorActivePath);
                            } catch (Exception e) {
                                Thread.sleep(1000);
                                return false;
                            }
                        } else {
                            return true;
                        }
                    }

                    return true;
                },
                Duration.ofMinutes(1),
                "error while cleaning coordinator server meta from ZooKeeper");

        waitUntil(
                () -> {
                    if (client.checkExists().forPath(tabletServerPath) != null) {
                        if (client.checkExists().forPath(idsPath) != null) {
                            List<String> idsChildren = client.getChildren().forPath(idsPath);
                            for (String child : idsChildren) {
                                String childPath = idsPath + "/" + child;
                                try {
                                    client.delete().forPath(childPath);
                                } catch (Exception e) {
                                    Thread.sleep(1000);
                                    return false;
                                }
                            }
                        } else {
                            return true;
                        }
                    }
                    return true;
                },
                Duration.ofMinutes(1),
                "error while cleaning tablet server meta from ZooKeeper");
    }
}
