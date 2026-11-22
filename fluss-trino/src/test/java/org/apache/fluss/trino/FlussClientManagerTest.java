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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests real client ownership and targeted initialization and cleanup failures. */
final class FlussClientManagerTest extends FlussTrinoTestBase {
    @Test
    void testPlaintextConfigurationAndCloseOrder() throws Exception {
        createDatabase("sales");
        List<String> closed = new ArrayList<>();
        AtomicInteger creations = new AtomicInteger();
        FlussClientManager manager =
                new FlussClientManager(
                        config().setSecurityProtocol("PLAINTEXT"),
                        configuration -> {
                            creations.incrementAndGet();
                            assertThat(configuration.toMap())
                                    .containsEntry(
                                            "bootstrap.servers", CLUSTER.getBootstrapServers())
                                    .containsEntry("client.security.protocol", "PLAINTEXT")
                                    .doesNotContainKeys(
                                            "client.security.sasl.mechanism",
                                            "client.security.sasl.username",
                                            "client.security.sasl.password");
                            Connection delegate = ConnectionFactory.createConnection(configuration);
                            TestingAdmin observedAdmin =
                                    new TestingAdmin(delegate) {
                                        @Override
                                        public void close() {
                                            super.close();
                                            closed.add("admin");
                                        }
                                    };
                            return new TestingConnection(delegate) {
                                @Override
                                public Admin getAdmin() {
                                    return observedAdmin;
                                }

                                @Override
                                public void close() throws Exception {
                                    super.close();
                                    closed.add("connection");
                                }
                            };
                        });

        try {
            assertThat(creations).hasValue(0);

            Admin initialized = manager.getAdmin();
            assertThat(manager.getAdmin()).isSameAs(initialized);
            assertThat(initialized.listDatabases().get(30, TimeUnit.SECONDS)).contains("sales");

            assertThat(creations).hasValue(1);
        } finally {
            manager.close();
        }

        manager.close();
        assertThat(closed).containsExactly("admin", "connection");
        assertThatThrownBy(manager::getAdmin)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void testInitializationFailurePreservesAuthenticationCause() throws Exception {
        RuntimeException failure =
                new RuntimeException(
                        "Failed to initialize Fluss client",
                        new AuthenticationException("Failed to create SASL client"));
        FlussClientManager manager =
                new FlussClientManager(
                        config(),
                        configuration -> {
                            throw failure;
                        });
        try {
            assertThatThrownBy(manager::getAdmin).isSameAs(failure);
        } finally {
            manager.close();
        }
    }

    @Test
    void testSlowTableOpenDoesNotBlockOtherCallers() throws Exception {
        TablePath path = TablePath.of("sales", "users");
        createTable(
                path,
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .build());
        CountDownLatch openStarted = new CountDownLatch(1);
        CountDownLatch releaseOpen = new CountDownLatch(1);
        Connection delayed =
                new TestingConnection(
                        ConnectionFactory.createConnection(CLUSTER.getClientConfig())) {
                    @Override
                    public Table getTable(TablePath tablePath) {
                        openStarted.countDown();
                        try {
                            assertThat(releaseOpen.await(30, TimeUnit.SECONDS)).isTrue();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        return super.getTable(tablePath);
                    }
                };
        FlussClientManager manager = new FlussClientManager(config(), configuration -> delayed);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Admin sharedAdmin = manager.getAdmin();
            Future<Table> opening = executor.submit(() -> manager.openTable(path));
            assertThat(openStarted.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(executor.submit(manager::getAdmin))
                    .succeedsWithin(Duration.ofSeconds(30))
                    .isSameAs(sharedAdmin);
            releaseOpen.countDown();
            try (Table table = opening.get(30, TimeUnit.SECONDS)) {
                assertThat(table.getTableInfo().getTablePath()).isEqualTo(path);
            }
        } finally {
            releaseOpen.countDown();
            executor.shutdownNow();
            manager.close();
        }
    }

    @Test
    void testPlaintextConfigurationDoesNotInitializeConnection() throws Exception {
        for (FlussConfig config :
                Arrays.asList(config(), config().setSecurityProtocol("PLAINTEXT"))) {
            FlussClientManager manager =
                    new FlussClientManager(
                            config,
                            configuration -> {
                                throw new AssertionError(
                                        "Construction and shutdown must not initialize a connection");
                            });
            manager.close();
        }
    }

    @Test
    void testCloseBeforeInitializationPreventsLaterUse() throws Exception {
        FlussClientManager manager =
                new FlussClientManager(
                        config(),
                        configuration -> {
                            throw new AssertionError(
                                    "A closed manager must not initialize a connection");
                        });
        manager.close();
        manager.close();
        assertThatThrownBy(manager::getAdmin)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        assertThatThrownBy(() -> manager.openTable(TablePath.of("sales", "users")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void testRetryAfterAdminInitializationFailure() throws Exception {
        createDatabase("sales");

        RuntimeException failure = new RuntimeException("cannot create admin");
        AtomicInteger failedCloses = new AtomicInteger();
        AtomicInteger creations = new AtomicInteger();

        FlussClientManager manager =
                new FlussClientManager(
                        config(),
                        configuration -> {
                            if (creations.getAndIncrement() == 0) {
                                return new TestingConnection(
                                        ConnectionFactory.createConnection(configuration)) {
                                    @Override
                                    public Admin getAdmin() {
                                        throw failure;
                                    }

                                    @Override
                                    public void close() throws Exception {
                                        super.close();
                                        failedCloses.incrementAndGet();
                                    }
                                };
                            }

                            return ConnectionFactory.createConnection(configuration);
                        });

        try {
            assertThatThrownBy(manager::getAdmin).isSameAs(failure);
            assertThat(failedCloses).hasValue(1);

            Admin initialized = manager.getAdmin();
            assertThat(initialized.listDatabases().get(30, TimeUnit.SECONDS)).contains("sales");
            assertThat(manager.getAdmin()).isSameAs(initialized);
            assertThat(creations).hasValue(2);
        } finally {
            manager.close();
        }

        assertThat(failedCloses).hasValue(1);
    }

    @Test
    void testCloseConnectionWhenAdminCreationFails() throws Exception {
        RuntimeException failure = new RuntimeException("cannot create admin");
        AtomicInteger closes = new AtomicInteger();

        FlussClientManager manager =
                new FlussClientManager(
                        config(),
                        configuration ->
                                new TestingConnection(
                                        ConnectionFactory.createConnection(configuration)) {
                                    @Override
                                    public Admin getAdmin() {
                                        throw failure;
                                    }

                                    @Override
                                    public void close() throws Exception {
                                        try {
                                            super.close();
                                        } finally {
                                            closes.incrementAndGet();
                                        }
                                    }
                                });

        try {
            assertThatThrownBy(manager::getAdmin).isSameAs(failure);
            assertThat(closes).hasValue(1);
        } finally {
            manager.close();
        }

        assertThat(closes).hasValue(1);
    }

    @Test
    void testInitializationFailureRetainsCleanupFailure() throws Exception {
        RuntimeException failure = new RuntimeException("cannot create admin");
        IOException cleanupFailure = new IOException("cannot close connection");
        FlussClientManager manager =
                new FlussClientManager(
                        config(),
                        configuration ->
                                new TestingConnection(
                                        ConnectionFactory.createConnection(configuration)) {
                                    @Override
                                    public Admin getAdmin() {
                                        throw failure;
                                    }

                                    @Override
                                    public void close() throws Exception {
                                        super.close();
                                        throw cleanupFailure;
                                    }
                                });

        try {
            assertThatThrownBy(manager::getAdmin).isSameAs(failure);
            assertThat(failure.getSuppressed()).containsExactly(cleanupFailure);
        } finally {
            manager.close();
        }
    }

    @Test
    void testCloseAttemptsBothResources() throws Exception {
        RuntimeException adminFailure = new RuntimeException("cannot close admin");
        IOException connectionFailure = new IOException("cannot close connection");
        List<String> closed = new ArrayList<>();
        Connection delegate = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
        TestingAdmin failingAdmin =
                new TestingAdmin(delegate) {
                    @Override
                    public void close() {
                        super.close();
                        closed.add("admin");
                        throw adminFailure;
                    }
                };
        Connection failingConnection =
                new TestingConnection(delegate) {
                    @Override
                    public Admin getAdmin() {
                        return failingAdmin;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        closed.add("connection");
                        throw connectionFailure;
                    }
                };
        FlussClientManager manager =
                new FlussClientManager(config(), configuration -> failingConnection);
        assertThat(manager.getAdmin()).isSameAs(failingAdmin);
        assertThatThrownBy(manager::close).isSameAs(adminFailure);
        assertThat(adminFailure.getSuppressed()).containsExactly(connectionFailure);
        manager.close();
        assertThat(closed).containsExactly("admin", "connection");
    }

    private static FlussConfig config() {
        return new FlussConfig().setBootstrapServers(CLUSTER.getBootstrapServers());
    }
}
