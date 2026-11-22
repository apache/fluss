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

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadManager;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadManager.PreparedCommit;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** A coordinator channel manager for test purpose which can set gateways manually. */
public class TestCoordinatorChannelManager extends CoordinatorChannelManager {

    private Map<Integer, TabletServerGateway> gateways;

    public TestCoordinatorChannelManager() {
        this(Collections.emptyMap());
    }

    public TestCoordinatorChannelManager(Map<Integer, TabletServerGateway> gateways) {
        super(
                RpcClient.create(new Configuration(), TestingClientMetricGroup.newInstance()),
                () -> 0,
                new Configuration(),
                TestingMetricGroups.COORDINATOR_METRICS);
        this.gateways = gateways;
    }

    /** Publishes through the real Coordinator while its leader notification channel is absent. */
    public static void publishWithDisconnectedLeader(
            CoordinatorEventProcessor processor,
            BulkLoadManager manager,
            String bulkLoadId,
            TableBucket bucket,
            ServerNode leader)
            throws Exception {
        CoordinatorChannelManager channels = processor.getCoordinatorChannelManager();
        PreparedCommit prepared =
                manager.prepareBucketCommit(
                                bulkLoadId,
                                bucket,
                                serverId -> channels.bulkLoadOffsets(serverId, bucket))
                        .get();
        channels.removeTabletServer(leader.id());
        try {
            processor.publishBulkLoadBucketSnapshot(prepared);
            // Keep the channel disconnected until the event thread has sent the new role.
            AccessContextEvent<Void> notificationSent = new AccessContextEvent<>(ctx -> null);
            processor.getCoordinatorEventManager().put(notificationSent);
            notificationSent.getResultFuture().get(30, TimeUnit.SECONDS);
        } finally {
            channels.addTabletServer(leader);
        }
    }

    public void setGateways(Map<Integer, TabletServerGateway> gateways) {
        this.gateways = gateways;
    }

    @Override
    protected Optional<TabletServerGateway> getTabletServerGateway(int serverId) {
        return Optional.ofNullable(gateways.get(serverId));
    }
}
