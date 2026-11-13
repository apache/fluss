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
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.MultiTable;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.Append;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.utils.IOUtils;

import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/** Shared catalog fixture with isolated per-test metadata and data clients. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(SAME_THREAD)
@ResourceLock("fluss-trino-cluster")
abstract class FlussTrinoTestBase {
    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .build();

    FlussClientManager clients;
    Admin admin;
    FlussMetadataAccess access;

    private Connection catalogConnection;
    private RpcClient sharedRpcClient;
    private MetadataUpdater testMetadataUpdater;
    private Connection dataConnection;

    @BeforeAll
    void openCatalog() {
        catalogConnection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
        try {
            admin = catalogConnection.getAdmin();
        } catch (RuntimeException | Error failure) {
            try {
                catalogConnection.close();
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            catalogConnection = null;
            throw failure;
        }
    }

    @BeforeEach
    void openClients() {
        testMetadataUpdater = null;
        dataConnection = null;

        clients =
                new FlussClientManager(
                        clientConfig(), ignored -> adminOnlyConnection(this::testingAdmin));
        access = new FlussMetadataAccess(clients);
    }

    @AfterEach
    void closeClients() throws Exception {
        try {
            IOUtils.closeAll(
                    () -> {
                        if (clients != null) {
                            clients.close();
                        }
                    },
                    dataConnection);
        } finally {
            clients = null;
            dataConnection = null;
            testMetadataUpdater = null;
            access = null;
        }
    }

    @AfterAll
    void closeCatalog() throws Exception {
        IOUtils.closeAll(admin, sharedRpcClient, catalogConnection);
    }

    FlussConfig clientConfig() {
        return new FlussConfig().setBootstrapServers(CLUSTER.getBootstrapServers());
    }

    /**
     * A fresh connection for actual data reads and writes. Created only when the test requests one.
     */
    Connection dataConnection() {
        if (dataConnection == null) {
            dataConnection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
        }
        return dataConnection;
    }

    /** The shared catalog connection must not own a test writer. */
    Connection catalogConnection() {
        return catalogConnection;
    }

    /**
     * Opens a table using the shared catalog connection for metadata and controlled scanner tests.
     * Real reads and writes use dataConnection().
     */
    Table catalogTable(TablePath path) {
        return new TestingScanTable(catalogConnection.getTable(path)) {
            @Override
            public Append newAppend() {
                throw new AssertionError("Use dataConnection() for writes");
            }

            @Override
            public Upsert newUpsert() {
                throw new AssertionError("Use dataConnection() for writes");
            }

            @Override
            public org.apache.fluss.client.lookup.Lookup newLookup() {
                throw new AssertionError("Use a per-test connection for lookups");
            }
        };
    }

    synchronized RpcClient sharedRpcClient() {
        if (sharedRpcClient == null) {
            sharedRpcClient =
                    RpcClient.create(
                            catalogConnection.getConfiguration(),
                            ((FlussConnection) catalogConnection).getClientMetricGroup());
        }
        return sharedRpcClient;
    }

    MetadataUpdater testMetadataUpdater() {
        if (testMetadataUpdater == null) {
            testMetadataUpdater = new MetadataUpdater(CLUSTER.getClientConfig(), sharedRpcClient());
        }
        return testMetadataUpdater;
    }

    TestingAdmin testingAdmin() {
        return new TestingAdmin(testMetadataUpdater(), sharedRpcClient());
    }

    private Connection adminOnlyConnection(Supplier<Admin> adminSupplier) {
        return new TestingConnection(catalogConnection) {
            @Override
            public Admin getAdmin() {
                return adminSupplier.get();
            }

            @Override
            public Table getTable(TablePath path) {
                throw new AssertionError("Use a test-specific Table for data access");
            }

            @Override
            public MultiTable getMultiTable() {
                throw new AssertionError("MultiTable is not supported by the catalog fixture");
            }

            @Override
            public void close() {
                // The test class owns the catalog connection.
            }
        };
    }

    void useAdmin(Admin replacement) throws Exception {
        clients.close();
        clients =
                new FlussClientManager(
                        clientConfig(), ignored -> adminOnlyConnection(() -> replacement));
        clients.getAdmin();
        access = new FlussMetadataAccess(clients);
    }

    void createDatabase(String name) throws Exception {
        admin.createDatabase(name, DatabaseDescriptor.EMPTY, true).get(30, TimeUnit.SECONDS);
    }

    TableInfo createTable(TablePath path, TableDescriptor descriptor) throws Exception {
        createDatabase(path.getDatabaseName());
        admin.createTable(path, descriptor, false).get(30, TimeUnit.SECONDS);

        TableInfo info = admin.getTableInfo(path).get(30, TimeUnit.SECONDS);

        if (!info.isPartitioned()) {
            CLUSTER.waitUntilTableReady(info.getTableId());
        }
        return info;
    }

    static FlussTableHandle tableHandle(TableInfo info) {
        TablePath path = info.getTablePath();
        return new FlussTableHandle(
                path.getDatabaseName().toLowerCase(Locale.ENGLISH),
                path.getTableName().toLowerCase(Locale.ENGLISH),
                path.getDatabaseName(),
                path.getTableName(),
                info.getTableId(),
                info.getSchemaId(),
                TupleDomain.all());
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        return config;
    }
}
