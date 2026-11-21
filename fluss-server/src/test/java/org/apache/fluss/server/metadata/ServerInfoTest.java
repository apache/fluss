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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ServerInfo}. */
class ServerInfoTest {

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "1.1-SNAPSHOT")
    void testNodeConversionsPreserveSoftwareVersion(@Nullable String softwareVersion) {
        ServerInfo info =
                new ServerInfo(
                        1,
                        "rack1",
                        Endpoint.fromListenersString("CLIENT://localhost:9000"),
                        ServerType.TABLET_SERVER,
                        TabletServerResource.unknown(),
                        softwareVersion);

        assertThat(info.node("CLIENT").softwareVersion()).isEqualTo(softwareVersion);
        assertThat(info.nodeOrThrow("CLIENT").softwareVersion()).isEqualTo(softwareVersion);
    }
}
