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

package com.alibaba.fluss.cluster;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ServerNode}. */
public class ServerNodeTest {

    @Test
    void testServerNode() {
        ServerNode serverNode = new ServerNode(0, "HOST1", 9023, ServerType.COORDINATOR);
        assertThat(serverNode.id()).isEqualTo(0);
        assertThat(serverNode.uid()).isEqualTo("cs-0");
        assertThat(serverNode.host()).isEqualTo("HOST1");
        assertThat(serverNode.port()).isEqualTo(9023);
        assertThat(serverNode.serverType()).isEqualTo(ServerType.COORDINATOR);
        assertThat(serverNode.isEmpty()).isFalse();

        ServerNode serverNode2 = new ServerNode(1, "HOST2", 9123, ServerType.TABLET_SERVER);
        assertThat(serverNode2.id()).isEqualTo(1);
        assertThat(serverNode2.uid()).isEqualTo("ts-1");
        assertThat(serverNode2.host()).isEqualTo("HOST2");
        assertThat(serverNode2.port()).isEqualTo(9123);
        assertThat(serverNode2.serverType()).isEqualTo(ServerType.TABLET_SERVER);
        assertThat(serverNode2.isEmpty()).isFalse();

        assertThat(serverNode.hashCode()).isNotEqualTo(serverNode2.hashCode());
        assertThat(serverNode).isEqualTo(new ServerNode(0, "HOST1", 9023, ServerType.COORDINATOR));

        assertThat(serverNode.toString()).isEqualTo("HOST1:9023 (id: cs-0)");
        assertThat(serverNode2.toString()).isEqualTo("HOST2:9123 (id: ts-1)");
    }
}
