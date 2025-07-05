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

package org.apache.fluss.cluster;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link Endpoint}. */
public class EndpointTest {

    @Test
    void testBindEndpoints() {
        assertThatThrownBy(
                        () ->
                                Endpoint.loadBindEndpoints(
                                        new Configuration()
                                                .set(
                                                        ConfigOptions.BIND_LISTENERS,
                                                        "INTERNAL://my_host:9092, CLIENT://127.0.0.1:9093, REPLICATION://[::1]:9094"),
                                        ServerType.TABLET_SERVER))
                .hasMessageContaining("Internal listener name FLUSS is not included in listeners");

        assertThatThrownBy(
                        () ->
                                Endpoint.loadBindEndpoints(
                                        new Configuration()
                                                .set(
                                                        ConfigOptions.BIND_LISTENERS,
                                                        "FLUSS://my_host:9092, CLIENT://127.0.0.1:9093, REPLICATION://[::1]:9092"),
                                        ServerType.TABLET_SERVER))
                .hasMessageContaining("There is more than one endpoint with the same port");

        List<Endpoint> bindEndpoints =
                Endpoint.loadBindEndpoints(
                        new Configuration()
                                .set(
                                        ConfigOptions.BIND_LISTENERS,
                                        "FLUSS://my_host:9092, CLIENT://127.0.0.1:9093, REPLICATION://[::1]:9094"),
                        ServerType.TABLET_SERVER);

        List<Endpoint> expectedEndpoints =
                Arrays.asList(
                        new Endpoint("my_host", 9092, "FLUSS"),
                        new Endpoint("127.0.0.1", 9093, "CLIENT"),
                        new Endpoint("::1", 9094, "REPLICATION"));
        assertThat(bindEndpoints).hasSameElementsAs(expectedEndpoints);
    }

    @Test
    void testAdvertisedEndpoints() {
        assertThatThrownBy(
                        () ->
                                Endpoint.loadAdvertisedEndpoints(
                                        Endpoint.fromListenersString(
                                                "INTERNAL://127.0.0.1:9092, CLIENT://127.0.0.1:9093, REPLICATION://[::1]:9094"),
                                        new Configuration()
                                                .set(
                                                        ConfigOptions.ADVERTISED_LISTENERS,
                                                        "CLIENT://my_host:9092,CLIENT2://my_host:9093,REPLICATION://[::1]:9094")))
                .hasMessageContaining(
                        "advertised listener name CLIENT2 is not included in listeners");
        List<Endpoint> registeredEndpoint =
                Endpoint.loadAdvertisedEndpoints(
                        Endpoint.fromListenersString(
                                "INTERNAL://127.0.0.1:9092, CLIENT://127.0.0.1:9093, REPLICATION://[::1]:9094"),
                        new Configuration()
                                .set(
                                        ConfigOptions.ADVERTISED_LISTENERS,
                                        "CLIENT://my_host:9092,REPLICATION://[::1]:9094"));
        List<Endpoint> expectedEndpoints =
                Arrays.asList(
                        new Endpoint("127.0.0.1", 9092, "INTERNAL"),
                        new Endpoint("my_host", 9092, "CLIENT"),
                        new Endpoint("::1", 9094, "REPLICATION"));
        assertThat(registeredEndpoint).hasSameElementsAs(expectedEndpoints);
    }

    @Test
    void testSamePorts() {
        assertThatThrownBy(
                        () ->
                                Endpoint.fromListenersString(
                                        "INTERNAL://my_host:9092, CLIENT://127.0.0.1:9093, REPLICATION://[::1]:9092"))
                .hasMessageContaining("There is more than one endpoint with the same port");
        // only port 0 is allowed to be duplicated.
        List<Endpoint> parsedEndpoint =
                Endpoint.fromListenersString(
                        "INTERNAL://my_host:0, CLIENT://127.0.0.1:9093, REPLICATION://[::1]:0");
        List<Endpoint> expectedEndpoints =
                Arrays.asList(
                        new Endpoint("my_host", 0, "INTERNAL"),
                        new Endpoint("127.0.0.1", 9093, "CLIENT"),
                        new Endpoint("::1", 0, "REPLICATION"));
        assertThat(parsedEndpoint).hasSameElementsAs(expectedEndpoints);
    }

    @ParameterizedTest
    @EnumSource(value = ServerType.class)
    void testCoordinatorEndpointsCompatibility(ServerType serverType) {
        Configuration configuration = new Configuration();
        assertThatThrownBy(() -> Endpoint.loadBindEndpoints(configuration, serverType))
                .hasMessageContaining("The 'bind.listeners' is not configured.");

        // if bind.listeners is not set, use deprecated [coordinator|tablet.server].host config
        // options even though [coordinator|tablet.server].port is not set.
        configuration.setString(
                serverType == ServerType.COORDINATOR
                        ? ConfigOptions.COORDINATOR_HOST
                        : ConfigOptions.TABLET_SERVER_HOST,
                "my_host");
        assertThat(Endpoint.loadBindEndpoints(configuration, serverType))
                .containsExactlyElementsOf(
                        Collections.singletonList(
                                new Endpoint(
                                        "my_host",
                                        serverType == ServerType.COORDINATOR ? 9123 : 0,
                                        "FLUSS")));

        configuration.setString(ConfigOptions.INTERNAL_LISTENER_NAME, "INTERNAL");
        configuration.setString(
                serverType == ServerType.COORDINATOR
                        ? ConfigOptions.COORDINATOR_PORT
                        : ConfigOptions.TABLET_SERVER_PORT,
                "9122");
        // if no bind.listeners setting, not allowed to set internal listener name as not FLUSS.
        assertThatThrownBy(() -> Endpoint.loadBindEndpoints(configuration, serverType))
                .hasMessageContaining(
                        "Config 'internal.listener.name' cannot be set without 'bind.listeners'");
        configuration.removeConfig(ConfigOptions.INTERNAL_LISTENER_NAME);
        // if bind.listeners is not set, use deprecated [coordinator|tablet.server].host config
        // options
        assertThat(Endpoint.loadBindEndpoints(configuration, serverType))
                .containsExactlyElementsOf(
                        Collections.singletonList(new Endpoint("my_host", 9122, "FLUSS")));
        // setting both bind.listeners and [coordinator|tablet.server].host is incompatible
        configuration.setString(ConfigOptions.BIND_LISTENERS, "FLUSS://127.0.0.1:9124");
        assertThatThrownBy(() -> Endpoint.loadBindEndpoints(configuration, serverType))
                .hasMessageContaining("Config options are incompatible.");
        // if [coordinator|tablet.server].host is not set and bind.listeners is set, use
        // bind.listeners
        configuration.removeConfig(
                serverType == ServerType.COORDINATOR
                        ? ConfigOptions.COORDINATOR_HOST
                        : ConfigOptions.TABLET_SERVER_HOST);
        configuration.setString(ConfigOptions.BIND_LISTENERS, "FLUSS://127.0.0.1:9124");
        assertThat(Endpoint.loadBindEndpoints(configuration, serverType))
                .containsExactlyElementsOf(
                        Collections.singletonList(new Endpoint("127.0.0.1", 9124, "FLUSS")));
    }
}
