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

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.server.metadata.ServerMetadataCacheImpl}. */
public class ServerMetadataCacheImplTest {
    private ServerMetadataCache serverMetadataCache;
    private ServerNode coordinatorServer;
    private Set<ServerNode> aliveTableServers;

    @BeforeEach
    public void setup() {
        serverMetadataCache = new ServerMetadataCacheImpl();
        coordinatorServer = new ServerNode(0, "localhost", 98, ServerType.COORDINATOR);
        aliveTableServers =
                new HashSet<>(
                        Arrays.asList(
                                new ServerNode(0, "localhost", 99, ServerType.TABLET_SERVER),
                                new ServerNode(1, "localhost", 100, ServerType.TABLET_SERVER),
                                new ServerNode(2, "localhost", 101, ServerType.TABLET_SERVER)));
    }

    @Test
    void testUpdateMetadataRequest() {
        serverMetadataCache.updateMetadata(
                new ClusterMetadataInfo(Optional.of(coordinatorServer), aliveTableServers));
        assertThat(serverMetadataCache.getCoordinatorServer()).isEqualTo(coordinatorServer);
        assertThat(serverMetadataCache.isAliveTabletServer(0)).isTrue();
        assertThat(serverMetadataCache.getAllAliveTabletServers().size()).isEqualTo(3);
    }
}
