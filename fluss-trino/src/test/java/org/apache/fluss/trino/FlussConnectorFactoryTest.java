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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;

import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Collections;

import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests the real connector bootstrap and lifecycle with a mocked external connection. */
final class FlussConnectorFactoryTest {
    @Test
    void testBootstrapMetadataAndShutdown() throws Exception {
        ConnectorContext context = mock(ConnectorContext.class, RETURNS_MOCKS);
        when(context.getSpiVersion()).thenReturn(new ConnectorContext() {}.getSpiVersion());
        Connection connection = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(connection.getAdmin()).thenReturn(admin);
        when(admin.listDatabases()).thenReturn(completedFuture(Collections.singletonList("Sales")));

        try (MockedStatic<ConnectionFactory> factory = mockStatic(ConnectionFactory.class)) {
            factory.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(connection);
            Connector connector =
                    new FlussConnectorFactory()
                            .create(
                                    "fluss",
                                    Collections.singletonMap("bootstrap.servers", "localhost:9123"),
                                    context);
            try {
                ConnectorMetadata metadata =
                        connector.getMetadata(
                                mock(ConnectorSession.class),
                                connector.beginTransaction(READ_COMMITTED, true, true));
                assertThat(metadata).isInstanceOf(ClassLoaderSafeConnectorMetadata.class);
                assertThat(metadata.listSchemaNames(mock(ConnectorSession.class)))
                        .containsExactly("sales");
                assertThat(connector.getTableProperties()).isNotEmpty();
            } finally {
                connector.shutdown();
            }
            verify(admin).close();
            verify(connection).close();
        }
    }

    @Test
    void testRejectMismatchedSpiVersion() {
        ConnectorContext context = mock(ConnectorContext.class);
        when(context.getSpiVersion()).thenReturn("incompatible-version");
        assertThatThrownBy(
                        () ->
                                new FlussConnectorFactory()
                                        .create("fluss", Collections.emptyMap(), context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unsupported Trino SPI version");
    }
}
