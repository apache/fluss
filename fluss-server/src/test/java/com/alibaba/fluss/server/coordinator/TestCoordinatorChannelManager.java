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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/** A coordinator channel manager for test purpose which can set gateways manually. */
public class TestCoordinatorChannelManager extends CoordinatorChannelManager {

    private Map<Integer, TabletServerGateway> gateways;

    public TestCoordinatorChannelManager() {
        this(Collections.emptyMap());
    }

    public TestCoordinatorChannelManager(Map<Integer, TabletServerGateway> gateways) {
        super(RpcClient.create(new Configuration(), TestingClientMetricGroup.newInstance()));
        this.gateways = gateways;
    }

    public void setGateways(Map<Integer, TabletServerGateway> gateways) {
        this.gateways = gateways;
    }

    protected Optional<TabletServerGateway> getTabletServerGateway(int serverId) {
        return Optional.ofNullable(gateways.get(serverId));
    }
}
