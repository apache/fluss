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

package com.alibaba.fluss.config;

import com.alibaba.fluss.cluster.Endpoint;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Endpoint}. */
public class EndPointTest {

    @Test
    void testParseEndpoints() {
        List<Endpoint> parsedEndpoints =
                Endpoint.parseEndpoints(
                        "INTERNAL://my_host:9092, CLIENT://127.0.0.1:9093, REPLICATION://[::1]:9092");
        List<Endpoint> expectedEndpoints =
                Arrays.asList(
                        new Endpoint("my_host", 9092, "INTERNAL"),
                        new Endpoint("127.0.0.1", 9093, "CLIENT"),
                        new Endpoint("::1", 9092, "REPLICATION"));

        assertThat(parsedEndpoints).hasSameElementsAs(expectedEndpoints);
    }

    @Test
    void testAdvisedEndpoints() {

        List<Endpoint> registeredEndpoint =
                Endpoint.getRegisteredEndpoint(
                        Endpoint.parseEndpoints(
                                "INTERNAL://127.0.0.1:9092, CLIENT://127.0.0.1:9093, REPLICATION://[::1]:9094"),
                        Endpoint.parseEndpoints(
                                "CLIENT://my_host:9092,CLIENT2://my_host:9093,REPLICATION://[::1]:9094"));
        List<Endpoint> expectedEndpoints =
                Arrays.asList(
                        new Endpoint("127.0.0.1", 9092, "INTERNAL"),
                        new Endpoint("my_host", 9092, "CLIENT"),
                        new Endpoint("::1", 9094, "REPLICATION"));
        assertThat(registeredEndpoint).hasSameElementsAs(expectedEndpoints);
    }
}
