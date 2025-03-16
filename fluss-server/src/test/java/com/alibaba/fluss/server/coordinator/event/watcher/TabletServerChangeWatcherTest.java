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

package com.alibaba.fluss.server.coordinator.event.watcher;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.server.coordinator.event.CoordinatorEvent;
import com.alibaba.fluss.server.coordinator.event.DeadTabletServerEvent;
import com.alibaba.fluss.server.coordinator.event.NewTabletServerEvent;
import com.alibaba.fluss.server.coordinator.event.TestingEventManager;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.server.zk.data.TabletServerRegistration;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TabletServerChangeWatcher} . */
class TabletServerChangeWatcherTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @Test
    void testServetChanges() throws Exception {
        ZooKeeperClient zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        TestingEventManager eventManager = new TestingEventManager();
        TabletServerChangeWatcher tabletServerChangeWatcher =
                new TabletServerChangeWatcher(zookeeperClient, eventManager);
        tabletServerChangeWatcher.start();

        // register new servers
        List<CoordinatorEvent> expectedEvents = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TabletServerRegistration tabletServerRegistration =
                    new TabletServerRegistration("host" + i, 1234, System.currentTimeMillis());
            expectedEvents.add(
                    new NewTabletServerEvent(
                            new ServerNode(
                                    i,
                                    tabletServerRegistration.getHost(),
                                    tabletServerRegistration.getPort(),
                                    ServerType.TABLET_SERVER)));
            zookeeperClient.registerTabletServer(i, tabletServerRegistration);
        }

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventManager.getEvents())
                                .containsExactlyInAnyOrderElementsOf(expectedEvents));

        // close it to mock the servers become down
        zookeeperClient.close();

        // unregister servers
        for (int i = 0; i < 10; i++) {
            expectedEvents.add(new DeadTabletServerEvent(i));
        }

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventManager.getEvents())
                                .containsExactlyInAnyOrderElementsOf(expectedEvents));

        tabletServerChangeWatcher.stop();
    }
}
