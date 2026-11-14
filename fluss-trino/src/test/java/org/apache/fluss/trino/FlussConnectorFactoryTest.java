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

package org.apache.fluss.trino;

import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableMap;

import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests connector bootstrap and lifecycle against a real Fluss catalog. */
final class FlussConnectorFactoryTest extends FlussTrinoTestBase {

    @Test
    void testBootstrapMetadataAndShutdown() throws Exception {
        createDatabase("Sales");

        Connector connector =
                new FlussConnectorFactory()
                        .create(
                                "fluss",
                                Collections.singletonMap(
                                        "bootstrap.servers", CLUSTER.getBootstrapServers()),
                                new TestingConnectorContext());

        ConnectorMetadata metadata;
        try {
            metadata =
                    connector.getMetadata(
                            SESSION, connector.beginTransaction(READ_COMMITTED, true, true));

            assertThat(metadata).isInstanceOf(ClassLoaderSafeConnectorMetadata.class);
            assertThat(metadata.listSchemaNames(SESSION)).contains("sales");
            assertThat(connector.getTableProperties()).isNotEmpty();

            assertThat(connector.getPageSourceProvider())
                    .isInstanceOf(ClassLoaderSafeConnectorPageSourceProvider.class);
            assertThat(connector.getSplitManager())
                    .isInstanceOf(ClassLoaderSafeConnectorSplitManager.class);
        } finally {
            connector.shutdown();
        }

        assertThatThrownBy(() -> metadata.listSchemaNames(SESSION))
                .isInstanceOf(TrinoException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Fluss client manager is closed");
    }

    @Test
    void testBootstrapAndShutdownWithoutFlussConnection() {
        Connector connector =
                new FlussConnectorFactory()
                        .create(
                                "fluss",
                                Collections.singletonMap("bootstrap.servers", "127.0.0.1:1"),
                                new TestingConnectorContext());
        connector.shutdown();
    }

    @Test
    void testRejectAuthenticationAtCatalogCreation() {
        for (Map.Entry<String, String> property :
                ImmutableMap.of(
                                "client.security.protocol", "SASL",
                                "client.security.sasl.mechanism", "PLAIN",
                                "client.security.sasl.username", "test-user",
                                "client.security.sasl.password", "test-password")
                        .entrySet()) {
            Map<String, String> config =
                    ImmutableMap.of(
                            "bootstrap.servers",
                            "127.0.0.1:1",
                            property.getKey(),
                            property.getValue());
            assertThatThrownBy(
                            () ->
                                    new FlussConnectorFactory()
                                            .create("fluss", config, new TestingConnectorContext()))
                    .hasMessageContaining("only PLAINTEXT connections are currently supported");
        }
    }

    @Test
    void testRejectMismatchedSpiVersion() {
        ConnectorContext context =
                new ConnectorContext() {
                    @Override
                    public String getSpiVersion() {
                        return "incompatible-version";
                    }
                };
        assertThatThrownBy(
                        () ->
                                new FlussConnectorFactory()
                                        .create("fluss", Collections.emptyMap(), context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unsupported Trino SPI version");
    }
}
