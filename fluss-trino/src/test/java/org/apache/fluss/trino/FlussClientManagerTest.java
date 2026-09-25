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

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests client ownership without creating network connections. */
final class FlussClientManagerTest {
    @Test
    void testConfigurationAndCloseOrder() throws Exception {
        Connection connection = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(connection.getAdmin()).thenReturn(admin);
        try (MockedStatic<ConnectionFactory> factory = mockStatic(ConnectionFactory.class)) {
            factory.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenAnswer(
                            invocation -> {
                                Configuration config = invocation.getArgument(0);
                                assertThat(config.toMap())
                                        .containsEntry("bootstrap.servers", "localhost:9123")
                                        .containsEntry("client.security.protocol", "SASL")
                                        .containsEntry("client.security.sasl.mechanism", "PLAIN")
                                        .containsEntry("client.security.sasl.username", "test-user")
                                        .containsEntry(
                                                "client.security.sasl.password", "test-password");
                                return connection;
                            });
            FlussClientManager manager =
                    new FlussClientManager(
                            new FlussConfig()
                                    .setBootstrapServers("localhost:9123")
                                    .setSecurityProtocol("SASL")
                                    .setSaslMechanism("PLAIN")
                                    .setSaslUsername("test-user")
                                    .setSaslPassword("test-password"));
            assertThat(manager.getAdmin()).isSameAs(admin);
            assertThat(manager.getAdmin()).isSameAs(admin);
            manager.close();
            InOrder order = inOrder(admin, connection);
            order.verify(admin).close();
            order.verify(connection).close();
            verify(connection).getAdmin();
        }
    }

    @Test
    void testCloseConnectionWhenAdminCreationFails() throws Exception {
        Connection connection = mock(Connection.class);
        RuntimeException failure = new RuntimeException("cannot create admin");
        when(connection.getAdmin()).thenThrow(failure);
        try (MockedStatic<ConnectionFactory> factory = mockStatic(ConnectionFactory.class)) {
            factory.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(connection);
            assertThatThrownBy(
                            () ->
                                    new FlussClientManager(
                                            new FlussConfig()
                                                    .setBootstrapServers("localhost:9123")))
                    .isSameAs(failure);
            verify(connection).close();
        }
    }

    @Test
    void testInitializationFailureRetainsCleanupFailure() throws Exception {
        Connection connection = mock(Connection.class);
        RuntimeException failure = new RuntimeException("cannot create admin");
        IOException cleanupFailure = new IOException("cannot close connection");
        when(connection.getAdmin()).thenThrow(failure);
        doThrow(cleanupFailure).when(connection).close();
        try (MockedStatic<ConnectionFactory> factory = mockStatic(ConnectionFactory.class)) {
            factory.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(connection);
            assertThatThrownBy(
                            () ->
                                    new FlussClientManager(
                                            new FlussConfig()
                                                    .setBootstrapServers("localhost:9123")))
                    .isSameAs(failure);
            assertThat(failure.getSuppressed()).containsExactly(cleanupFailure);
        }
    }

    @Test
    void testCloseAttemptsBothResources() throws Exception {
        Connection connection = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(connection.getAdmin()).thenReturn(admin);
        IOException adminFailure = new IOException("cannot close admin");
        IOException connectionFailure = new IOException("cannot close connection");
        doThrow(adminFailure).when(admin).close();
        doThrow(connectionFailure).when(connection).close();
        try (MockedStatic<ConnectionFactory> factory = mockStatic(ConnectionFactory.class)) {
            factory.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(connection);
            FlussClientManager manager =
                    new FlussClientManager(new FlussConfig().setBootstrapServers("localhost:9123"));
            assertThatThrownBy(manager::close).isSameAs(adminFailure);
            assertThat(adminFailure.getSuppressed()).containsExactly(connectionFailure);
            verify(connection).close();
        }
    }
}
