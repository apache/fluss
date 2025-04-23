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

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
        return new TabletServerRegistration[] {
            tabletServerRegistration1, tabletServerRegistration2
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":3,\"listeners\":\"CLIENT://localhost:2345,FLUSS://127.0.0.1:2346\",\"register_timestamp\":10000}",
            "{\"version\":3,\"listeners\":\"CLIENT://localhost:2345,FLUSS://127.0.0.1:2346\",\"register_timestamp\":10000,\"rack\":\"cn-hangzhou-server10\"}"
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
        assertEquals(tabletServerRegistration, expectedTabletServerRegistration);

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
        assertEquals(tabletServerRegistration, expectedTabletServerRegistration);
    }
}
