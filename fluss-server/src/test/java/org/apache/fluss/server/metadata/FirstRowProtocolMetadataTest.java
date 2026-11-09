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

package org.apache.fluss.server.metadata;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FirstRowProtocolMetadataTest {
    @Test
    void testClusterSupportRequiresEveryMember() {
        Map<Integer, ServerInfo> nodes = new HashMap<>();
        nodes.put(0, server(0, 4));
        assertThat(snapshot(nodes).supportsFirstRowProgress()).isTrue();
        nodes.put(1, server(1, 0));
        assertThat(snapshot(nodes).supportsFirstRowProgress()).isFalse();
        nodes.put(1, server(1, 4));
        assertThat(snapshot(nodes).supportsFirstRowProgress()).isTrue();
        assertThat(ServerMetadataSnapshot.empty().supportsFirstRowProgress()).isFalse();
    }

    @Test
    void testMembershipProtocolVersionRoundTrip() {
        ServerInfo modern = server(0, 4);
        UpdateMetadataRequest request =
                ServerRpcMessageUtils.makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.singleton(modern),
                        Collections.emptyList(),
                        Collections.emptyList());
        assertThat(
                        ServerRpcMessageUtils.getUpdateMetadataRequestData(request)
                                .getAliveTabletServers())
                .containsExactly(modern);
        ServerInfo legacy = server(1, 0);
        request =
                ServerRpcMessageUtils.makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.singleton(legacy),
                        Collections.emptyList(),
                        Collections.emptyList());
        assertThat(request.getTabletServersList().get(0).hasMaxPutKvVersion()).isFalse();
        assertThat(
                        ServerRpcMessageUtils.getUpdateMetadataRequestData(request)
                                .getAliveTabletServers())
                .containsExactly(legacy);
    }

    private ServerInfo server(int id, int version) {
        return new ServerInfo(
                id,
                null,
                Collections.singletonList(new Endpoint("localhost", 1234 + id, "INTERNAL")),
                ServerType.TABLET_SERVER,
                TabletServerResource.unknown(),
                version);
    }

    private ServerMetadataSnapshot snapshot(Map<Integer, ServerInfo> nodes) {
        return new ServerMetadataSnapshot(
                null,
                new HashMap<>(nodes),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap());
    }
}
