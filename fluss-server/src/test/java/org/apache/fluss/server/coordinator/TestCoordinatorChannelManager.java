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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongConsumer;

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

    public void setGateways(Map<Integer, TabletServerGateway> gateways) {
        this.gateways = gateways;
    }

    Optional<TabletServerGateway> getTabletServerGateway(int serverId) {
        return Optional.ofNullable(gateways.get(serverId));
    }

    @Override
    protected <ResponseT extends ApiMessage> boolean enqueueRequest(
            int targetServerId,
            ApiKeys apiKey,
            int coordinatorEpoch,
            Function<TabletServerGateway, CompletableFuture<ResponseT>> requestSender,
            @Nullable BiConsumer<ResponseT, ? super Throwable> responseConsumer) {
        return sendRequest(targetServerId, requestSender, responseConsumer);
    }

    @Override
    protected <ResponseT extends ApiMessage> boolean enqueueRequest(
            int targetServerId,
            ApiKeys apiKey,
            int coordinatorEpoch,
            @Nullable LongConsumer tabletServerEpochSetter,
            Function<TabletServerGateway, CompletableFuture<ResponseT>> requestSender,
            @Nullable BiConsumer<ResponseT, ? super Throwable> responseConsumer) {
        if (tabletServerEpochSetter != null) {
            tabletServerEpochSetter.accept(0L);
        }
        return sendRequest(targetServerId, requestSender, responseConsumer);
    }

    private <ResponseT extends ApiMessage> boolean sendRequest(
            int targetServerId,
            Function<TabletServerGateway, CompletableFuture<ResponseT>> requestSender,
            @Nullable BiConsumer<ResponseT, ? super Throwable> responseConsumer) {
        TabletServerGateway gateway = gateways.get(targetServerId);
        if (gateway == null) {
            return false;
        }
        CompletableFuture<ResponseT> responseFuture = requestSender.apply(gateway);
        if (responseConsumer != null) {
            responseFuture.whenComplete(responseConsumer);
        }
        return true;
    }
}
