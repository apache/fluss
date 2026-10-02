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

package org.apache.fluss.server.tablet;

import org.apache.fluss.exception.FencedTabletServerEpochException;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TabletService}. */
class TabletServiceTest {

    @Test
    void testFenceControlRequestsQueuedBeforeTabletServerReregistration() {
        long requestEpoch = 1L;
        AtomicLong currentEpoch = new AtomicLong(requestEpoch);
        ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();
        TabletService tabletService = createTabletService(executor, currentEpoch);

        List<CompletableFuture<?>> responses =
                Arrays.asList(
                        tabletService.notifyLeaderAndIsr(
                                new NotifyLeaderAndIsrRequest()
                                        .setCoordinatorEpoch(0)
                                        .setTabletServerEpoch(requestEpoch)),
                        tabletService.updateMetadata(
                                new UpdateMetadataRequest()
                                        .setCoordinatorEpoch(0)
                                        .setTabletServerEpoch(requestEpoch)),
                        tabletService.stopReplica(
                                new StopReplicaRequest()
                                        .setCoordinatorEpoch(0)
                                        .setTabletServerEpoch(requestEpoch)));

        assertThat(executor.numQueuedRunnables()).isEqualTo(3);
        assertThat(responses).allSatisfy(response -> assertThat(response.isDone()).isFalse());

        currentEpoch.incrementAndGet();
        executor.triggerAll();

        assertThat(responses)
                .allSatisfy(
                        response ->
                                assertThatThrownBy(response::join)
                                        .hasCauseInstanceOf(
                                                FencedTabletServerEpochException.class));
    }

    private static TabletService createTabletService(
            ManuallyTriggeredScheduledExecutorService executor, AtomicLong currentEpoch) {
        return new TabletService(
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                executor,
                executor,
                null,
                null,
                currentEpoch::get,
                "CLIENT");
    }
}
