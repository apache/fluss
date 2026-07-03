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

import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link CoordinatorRequestBatch}. */
class CoordinatorRequestBatchTest {

    private CoordinatorContext coordinatorContext;

    @BeforeEach
    void beforeEach() {
        coordinatorContext = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
    }

    /**
     * When the NotifyLeaderAndIsr request to the actual leader (leader == serverId) fails, the
     * pending leader activation entry that this request added must be cleared via an
     * AccessContextEvent dispatched on the event manager.
     */
    @Test
    void testNotifyLeaderAndIsrSendFailureClearsLeaderPending() {
        long tableId = 100L;
        TableBucket tb = new TableBucket(tableId, 0);
        TablePath tablePath = TablePath.of("db1", "t1");

        coordinatorContext.putTablePath(tableId, tablePath);
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Collections.singletonList(0)));
        coordinatorContext.updateBucketReplicaAssignment(tb, Collections.singletonList(0));
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Collections.singletonList(0), Collections.emptyList(), 0, 0);
        coordinatorContext.putBucketLeaderAndIsr(tb, leaderAndIsr);

        TestCoordinatorChannelManager failingChannel = newAlwaysFailingChannelManager();
        EventManager eventManager = newSynchronousAccessContextEventManager();

        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(failingChannel, eventManager, coordinatorContext);
        // server 0 IS the leader, so this request will mark tb pending.
        batch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(0),
                PhysicalTablePath.of(tablePath),
                tb,
                Collections.singletonList(0),
                leaderAndIsr);

        batch.sendRequestToTabletServers(0);

        // The failure callback must clear the pending entry that this request added.
        assertThat(coordinatorContext.getPendingLeaderActivationBuckets()).isEmpty();
    }

    /**
     * When the NotifyLeaderAndIsr request to a follower (leader != serverId) fails, the failure
     * callback must NOT clear any pending leader activation entry. Specifically, it must not remove
     * entries added by another in-flight sender targeting the actual leader.
     */
    @Test
    void testNotifyLeaderAndIsrSendFailureToFollowerDoesNotClearOtherPending() {
        long tableId = 200L;
        TableBucket followerTb = new TableBucket(tableId, 0);
        TableBucket otherLeaderTb = new TableBucket(tableId, 1);
        TablePath tablePath = TablePath.of("db1", "t2");

        coordinatorContext.putTablePath(tableId, tablePath);
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Arrays.asList(0, 1)));
        coordinatorContext.updateBucketReplicaAssignment(followerTb, Arrays.asList(0, 1));
        coordinatorContext.updateBucketReplicaAssignment(otherLeaderTb, Arrays.asList(0, 1));
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Arrays.asList(0, 1), Collections.emptyList(), 0, 0);
        coordinatorContext.putBucketLeaderAndIsr(followerTb, leaderAndIsr);

        // Simulate another in-flight sender having already marked otherLeaderTb pending - we
        // must not touch this entry on the unrelated follower-send failure.
        coordinatorContext.addPendingLeaderActivation(otherLeaderTb);

        TestCoordinatorChannelManager failingChannel = newAlwaysFailingChannelManager();
        EventManager eventManager = newSynchronousAccessContextEventManager();

        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(failingChannel, eventManager, coordinatorContext);
        // Send to server 1, which is a follower for followerTb (leader is 0). Because
        // leader != serverId, this request does NOT add followerTb to pending, so the failure
        // callback must short-circuit without dispatching an AccessContextEvent.
        batch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(1),
                PhysicalTablePath.of(tablePath),
                followerTb,
                Arrays.asList(0, 1),
                leaderAndIsr);

        batch.sendRequestToTabletServers(0);

        // The unrelated leader's pending entry must remain intact.
        assertThat(coordinatorContext.getPendingLeaderActivationBuckets())
                .containsExactly(otherLeaderTb);
    }

    private static TestCoordinatorChannelManager newAlwaysFailingChannelManager() {
        return new TestCoordinatorChannelManager() {
            @Override
            public void sendBucketLeaderAndIsrRequest(
                    int receiveServerId,
                    NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest,
                    BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable> responseConsumer) {
                responseConsumer.accept(
                        null, new NetworkException("simulated send failure for test"));
            }
        };
    }

    /**
     * Returns an EventManager that synchronously executes {@link AccessContextEvent}s against the
     * test's CoordinatorContext, mimicking the serial event-thread semantics.
     */
    private EventManager newSynchronousAccessContextEventManager() {
        return event -> {
            if (event instanceof AccessContextEvent) {
                AccessContextEvent<?> accessContextEvent = (AccessContextEvent<?>) event;
                accessContextEvent.getAccessFunction().apply(coordinatorContext);
            }
        };
    }
}
