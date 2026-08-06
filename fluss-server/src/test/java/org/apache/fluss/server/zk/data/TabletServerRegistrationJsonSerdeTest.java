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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.apache.fluss.server.zk.data.TabletServerRegistration.REMOTE_MANIFEST_VERSION_DISPATCH_CAPABILITY;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TabletServerRegistrationJsonSerde}. */
public class TabletServerRegistrationJsonSerdeTest
        extends JsonSerdeTestBase<TabletServerRegistration> {

    TabletServerRegistrationJsonSerdeTest() {
        super(TabletServerRegistrationJsonSerde.INSTANCE);
    }

    @Override
    protected TabletServerRegistration[] createObjects() {
        TabletServerRegistration tabletServerRegistration1 =
                new TabletServerRegistration(
                        null,
                        Endpoint.fromListenersString(
                                "CLIENT://localhost:2345,FLUSS://127.0.0.1:2346"),
                        10000);
        TabletServerRegistration tabletServerRegistration2 =
                new TabletServerRegistration(
                        "cn-hangzhou-server10",
                        Endpoint.fromListenersString(
                                "CLIENT://localhost:2345,FLUSS://127.0.0.1:2346"),
                        10000);
        TabletServerRegistration tabletServerRegistration3 =
                new TabletServerRegistration(
                        "cn-hangzhou-server10",
                        Endpoint.fromListenersString(
                                "CLIENT://localhost:2345,FLUSS://127.0.0.1:2346"),
                        10000,
                        new TabletServerResource(8.0, 1024L));
        TabletServerRegistration tabletServerRegistration4 =
                new TabletServerRegistration(
                        "cn-hangzhou-server10",
                        Endpoint.fromListenersString(
                                "CLIENT://localhost:2345,FLUSS://127.0.0.1:2346"),
                        10000,
                        new TabletServerResource(8.0, 1024L),
                        Collections.singleton(REMOTE_MANIFEST_VERSION_DISPATCH_CAPABILITY));
        return new TabletServerRegistration[] {
            tabletServerRegistration1,
            tabletServerRegistration2,
            tabletServerRegistration3,
            tabletServerRegistration4
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":5,\"listeners\":\"CLIENT://localhost:2345,FLUSS://127.0.0.1:2346\",\"register_timestamp\":10000,\"capabilities\":[]}",
            "{\"version\":5,\"listeners\":\"CLIENT://localhost:2345,FLUSS://127.0.0.1:2346\",\"register_timestamp\":10000,\"rack\":\"cn-hangzhou-server10\",\"capabilities\":[]}",
            "{\"version\":5,\"listeners\":\"CLIENT://localhost:2345,FLUSS://127.0.0.1:2346\",\"register_timestamp\":10000,\"rack\":\"cn-hangzhou-server10\",\"cpu_cores\":8.0,\"memory_bytes\":1024,\"capabilities\":[]}",
            "{\"version\":5,\"listeners\":\"CLIENT://localhost:2345,FLUSS://127.0.0.1:2346\",\"register_timestamp\":10000,\"rack\":\"cn-hangzhou-server10\",\"cpu_cores\":8.0,\"memory_bytes\":1024,\"capabilities\":[\"remote-manifest-version-dispatch\"]}"
        };
    }

    @Test
    void testCompatibility() throws IOException {
        // compatibility with version 1
        JsonNode jsonInVersion1 =
                new ObjectMapper()
                        .readTree(
                                "{\"version\":1,\"host\":\"localhost\",\"port\":1001,\"register_timestamp\":10000}"
                                        .getBytes(StandardCharsets.UTF_8));

        TabletServerRegistration tabletServerRegistration =
                TabletServerRegistrationJsonSerde.INSTANCE.deserialize(jsonInVersion1);
        TabletServerRegistration expectedTabletServerRegistration =
                new TabletServerRegistration(
                        null, Endpoint.fromListenersString("FLUSS://localhost:1001"), 10000);
        assertThat(tabletServerRegistration).isEqualTo(expectedTabletServerRegistration);

        // compatibility with version 2
        JsonNode jsonInVersion2 =
                new ObjectMapper()
                        .readTree(
                                ("{\"version\":2,\"listeners\":\"CLIENT://localhost:2345,FLUSS://127.0.0.1:2346\","
                                                + "\"register_timestamp\":10000}")
                                        .getBytes(StandardCharsets.UTF_8));
        tabletServerRegistration =
                TabletServerRegistrationJsonSerde.INSTANCE.deserialize(jsonInVersion2);
        expectedTabletServerRegistration =
                new TabletServerRegistration(
                        null,
                        Endpoint.fromListenersString(
                                "CLIENT://localhost:2345,FLUSS://127.0.0.1:2346"),
                        10000);
        assertThat(tabletServerRegistration).isEqualTo(expectedTabletServerRegistration);

        // compatibility with version 3
        JsonNode jsonInVersion3 =
                new ObjectMapper()
                        .readTree(
                                ("{\"version\":3,\"listeners\":\"CLIENT://localhost:2345,FLUSS://127.0.0.1:2346\","
                                                + "\"register_timestamp\":10000,\"rack\":\"cn-hangzhou-server10\"}")
                                        .getBytes(StandardCharsets.UTF_8));
        tabletServerRegistration =
                TabletServerRegistrationJsonSerde.INSTANCE.deserialize(jsonInVersion3);
        expectedTabletServerRegistration =
                new TabletServerRegistration(
                        "cn-hangzhou-server10",
                        Endpoint.fromListenersString(
                                "CLIENT://localhost:2345,FLUSS://127.0.0.1:2346"),
                        10000);
        assertThat(tabletServerRegistration).isEqualTo(expectedTabletServerRegistration);

        // compatibility with version 4: a legacy registration has no capabilities.
        JsonNode jsonInVersion4 =
                new ObjectMapper()
                        .readTree(
                                ("{\"version\":4,\"listeners\":\"CLIENT://localhost:2345,FLUSS://127.0.0.1:2346\","
                                                + "\"register_timestamp\":10000,\"rack\":\"cn-hangzhou-server10\","
                                                + "\"cpu_cores\":8.0,\"memory_bytes\":1024}")
                                        .getBytes(StandardCharsets.UTF_8));
        tabletServerRegistration =
                TabletServerRegistrationJsonSerde.INSTANCE.deserialize(jsonInVersion4);
        assertThat(tabletServerRegistration.getResource())
                .isEqualTo(new TabletServerResource(8.0, 1024L));
        assertThat(tabletServerRegistration.getCapabilities()).isEmpty();
        assertThat(
                        tabletServerRegistration.supportsCapability(
                                REMOTE_MANIFEST_VERSION_DISPATCH_CAPABILITY))
                .isFalse();
    }
}
