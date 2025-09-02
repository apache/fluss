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

package org.apache.fluss.server.coordinator.event.watcher;

import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.DeadCoordinatorServerEvent;
import org.apache.fluss.server.coordinator.event.NewCoordinatorServerEvent;
import org.apache.fluss.server.coordinator.event.TestingEventManager;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CoordinatorServerChangeWatcher} . */
class CoordinatorServerChangeWatcherTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @Test
    void testServerChanges() throws Exception {
        ZooKeeperClient zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        TestingEventManager eventManager = new TestingEventManager();
        CoordinatorServerChangeWatcher coordinatorServerChangeWatcher =
                new CoordinatorServerChangeWatcher(zookeeperClient, eventManager);
        coordinatorServerChangeWatcher.start();

        // register new servers
        List<CoordinatorEvent> expectedEvents = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            expectedEvents.add(new NewCoordinatorServerEvent(i));
            zookeeperClient.registerCoordinatorServer(i);
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
            expectedEvents.add(new DeadCoordinatorServerEvent(i));
        }

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventManager.getEvents())
                                .containsExactlyInAnyOrderElementsOf(expectedEvents));

        coordinatorServerChangeWatcher.stop();
    }
}
