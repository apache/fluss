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

import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.concurrent.FutureUtils;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.fluss.trino.FlussErrorCode.FLUSS_METADATA_CHANGED;
import static org.apache.fluss.trino.FlussErrorCode.FLUSS_METADATA_ERROR;
import static org.apache.fluss.trino.FlussErrorCode.FLUSS_SPLIT_ERROR;
import static org.apache.fluss.trino.TestingFlussMetadata.usersTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests physical name resolution and catalog failure translation at the Admin boundary. */
final class FlussMetadataAccessTest extends FlussTrinoTestBase {
    private TablePath tablePath;

    @BeforeEach
    void setUpTablePath() {
        tablePath = TablePath.of(databaseName("Sales"), "Users");
    }

    @Test
    void testSchemaNamesAreCanonicalAndDistinct() throws Exception {
        createDatabase(databaseName("Sales"));
        createDatabase(databaseName("sales"));
        createDatabase(databaseName("Inventory"));
        assertThat(access.listSchemaNames())
                .contains(databaseName("sales"), databaseName("inventory"))
                .doesNotHaveDuplicates();
        assertThat(access.resolveSchema(databaseName("INVENTORY")))
                .hasValueSatisfying(
                        schema -> {
                            assertThat(schema.getTrinoName()).isEqualTo(databaseName("inventory"));
                            assertThat(schema.getFlussName()).isEqualTo(databaseName("Inventory"));
                        });
    }

    @Test
    void testAmbiguousSchema() throws Exception {
        createDatabase(databaseName("Sales"));
        createDatabase(databaseName("sales"));
        assertThatThrownBy(() -> access.resolveSchema(databaseName("sales")))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage(
                        "Ambiguous Fluss database '%s': %s, %s",
                        databaseName("sales"), databaseName("Sales"), databaseName("sales"));
        assertThatThrownBy(() -> access.listTableNameMappings(Optional.empty()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Ambiguous Fluss database");
    }

    @Test
    void testResolvePhysicalTableName() throws Exception {
        createTable(tablePath, usersTable().toTableDescriptor());
        assertThat(access.resolveTable(new SchemaTableName(databaseName("sales"), "users")))
                .hasValueSatisfying(
                        table -> {
                            assertThat(table.getTrinoName())
                                    .isEqualTo(new SchemaTableName(databaseName("sales"), "users"));
                            assertThat(table.getFlussDatabaseName())
                                    .isEqualTo(databaseName("Sales"));
                            assertThat(table.getFlussTableName()).isEqualTo("Users");
                        });
    }

    @Test
    void testCatalogWideListingResolvesEachPhysicalDatabase() throws Exception {
        createTable(tablePath, usersTable().toTableDescriptor());
        createTable(
                TablePath.of(databaseName("Inventory"), "Products"),
                usersTable().toTableDescriptor());
        assertThat(access.listTableNameMappings(Optional.empty()))
                .flatExtracting(FlussMetadataAccess.TableNameMapping::listTableNames)
                .containsExactlyInAnyOrder(
                        new SchemaTableName(databaseName("sales"), "users"),
                        new SchemaTableName(databaseName("inventory"), "products"));
    }

    @Test
    void testTargetedListingIgnoresUnrelatedSchemaAmbiguity() throws Exception {
        createDatabase(databaseName("Sales"));
        createDatabase(databaseName("sales"));
        createTable(
                TablePath.of(databaseName("Inventory"), "Products"),
                usersTable().toTableDescriptor());
        assertThat(access.listTableNameMappings(Optional.of(databaseName("inventory"))))
                .flatExtracting(FlussMetadataAccess.TableNameMapping::listTableNames)
                .containsExactly(new SchemaTableName(databaseName("inventory"), "products"));
    }

    @Test
    void testMissingSchema() throws Exception {
        assertThat(access.resolveSchema("missing")).isEmpty();
        assertThat(access.resolveTable(new SchemaTableName("missing", "users"))).isEmpty();
        assertThat(access.listTableNameMappings(Optional.of("missing"))).isEmpty();
    }

    @Test
    void testTableIndexResolvesOnlyRequestedNames() throws Exception {
        for (String name : Arrays.asList("Users", "Order", "oRder")) {
            createTable(
                    TablePath.of(databaseName("Sales"), name), usersTable().toTableDescriptor());
        }
        FlussMetadataAccess.TableNameMapping index =
                access.loadTableNameMapping(
                        new ResolvedSchemaName(databaseName("sales"), databaseName("Sales")));
        admin.dropDatabase(databaseName("Sales"), false, true).get(30, TimeUnit.SECONDS);
        assertThat(index.listTableNames())
                .containsExactlyInAnyOrder(
                        new SchemaTableName(databaseName("sales"), "users"),
                        new SchemaTableName(databaseName("sales"), "order"));
        assertThat(index.resolveTable("USERS"))
                .hasValueSatisfying(
                        table -> assertThat(table.getFlussTableName()).isEqualTo("Users"));
        assertThat(index.resolveTable("missing")).isEmpty();
        assertThatThrownBy(() -> index.resolveTable("order"))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Ambiguous Fluss table 'order': Order, oRder");
    }

    @Test
    void testDatabaseDroppedBeforeListingTables() throws Exception {
        createDatabase(databaseName("Sales"));
        ResolvedSchemaName schema = access.resolveSchema(databaseName("sales")).get();
        admin.dropDatabase(databaseName("Sales"), false, true).get(30, TimeUnit.SECONDS);
        assertThat(access.loadTableNameMapping(schema).listTableNames()).isEmpty();
    }

    @Test
    void testDiscoverySkipsDroppedTableOrDatabase() throws Exception {
        createTable(tablePath, usersTable().toTableDescriptor());
        ResolvedTableName table =
                access.resolveTable(new SchemaTableName(databaseName("sales"), "users")).get();
        admin.dropTable(tablePath, false).get(30, TimeUnit.SECONDS);
        assertThat(access.findTableInfo(table)).isEmpty();
        admin.dropDatabase(databaseName("Sales"), false, true).get(30, TimeUnit.SECONDS);
        assertThat(access.findTableInfo(table)).isEmpty();
    }

    @Test
    void testExistingHandleReportsDroppedTable() throws Exception {
        FlussTableHandle handle =
                tableHandle(createTable(tablePath, usersTable().toTableDescriptor()));
        admin.dropTable(tablePath, false).get(30, TimeUnit.SECONDS);
        assertThatThrownBy(() -> access.getTableInfo(handle))
                .isInstanceOf(TableNotFoundException.class)
                .hasCauseInstanceOf(TableNotExistException.class)
                .hasMessageContaining(databaseName("sales") + ".users");
    }

    @Test
    void testValidateTableAndSchemaIdentity() throws Exception {
        TableInfo info = createTable(tablePath, usersTable().toTableDescriptor());
        FlussTableHandle handle = tableHandle(info);
        assertThat(access.getTableInfo(handle)).isEqualTo(info);
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "extra",
                                        DataTypes.STRING(),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get(30, TimeUnit.SECONDS);
        assertThatThrownBy(() -> access.getTableInfo(handle))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(FLUSS_METADATA_CHANGED.toErrorCode()))
                .hasMessageContaining("changed");
        admin.dropTable(tablePath, false).get(30, TimeUnit.SECONDS);
        createTable(tablePath, usersTable().toTableDescriptor());
        assertThatThrownBy(() -> access.getTableInfo(handle))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("changed");
    }

    @Test
    void testListScanPartitionsPreservesPhysicalIdentityAndCounts() throws Exception {
        FlussTableHandle handle =
                tableHandle(createTable(tablePath, usersTable().toTableDescriptor()));
        PartitionSpec east = new PartitionSpec(Collections.singletonMap("Region", "east"));
        admin.createPartition(tablePath, east, false).get(30, TimeUnit.SECONDS);
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(TableChange.modifyBucketCount(2)),
                        false)
                .get(30, TimeUnit.SECONDS);
        admin.createPartition(
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("Region", "west")),
                        false)
                .get(30, TimeUnit.SECONDS);
        List<PartitionInfo> partitions = access.listScanPartitions(handle, Optional.empty());
        assertThat(partitions)
                .extracting(PartitionInfo::getBucketCount)
                .containsExactlyInAnyOrder(4, 2);
        assertThat(partitions).extracting(PartitionInfo::getPartitionId).doesNotHaveDuplicates();
        assertThatThrownBy(() -> partitions.add(partitionInfo(30, "north", 8)))
                .isInstanceOf(UnsupportedOperationException.class);
        admin.dropPartition(tablePath, east, false).get(30, TimeUnit.SECONDS);
        assertThat(partitions).hasSize(2);
        assertThat(access.listScanPartitions(handle, Optional.empty())).hasSize(1);
    }

    @Test
    void testListScanPartitionsUsesPartitionSpec() throws Exception {
        FlussTableHandle handle =
                tableHandle(createTable(tablePath, usersTable().toTableDescriptor()));
        PartitionSpec spec = new PartitionSpec(Collections.singletonMap("Region", "east"));
        admin.createPartition(tablePath, spec, false).get(30, TimeUnit.SECONDS);
        admin.createPartition(
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("Region", "west")),
                        false)
                .get(30, TimeUnit.SECONDS);
        assertThat(access.listScanPartitions(handle, Optional.of(spec)))
                .extracting(PartitionInfo::getPartitionName)
                .containsExactly("east");
    }

    @Test
    void testListScanPartitionsRejectsDuplicateIdentities() throws Exception {
        for (List<PartitionInfo> response :
                Arrays.asList(
                        Arrays.asList(partitionInfo(10, "east", 2), partitionInfo(10, "west", 4)),
                        Arrays.asList(
                                partitionInfo(10, "east", 2), partitionInfo(20, "east", 4)))) {
            usePartitions(response);
            assertThatThrownBy(
                            () ->
                                    access.listScanPartitions(
                                            tableHandle(usersTable()), Optional.empty()))
                    .isInstanceOfSatisfying(
                            TrinoException.class,
                            e ->
                                    assertThat(e.getErrorCode())
                                            .isEqualTo(FLUSS_SPLIT_ERROR.toErrorCode()))
                    .hasMessageContaining("Duplicate Fluss partition");
        }
    }

    @Test
    void testListScanPartitionsRejectsInvalidCount() throws Exception {
        for (int count : new int[] {0, -1}) {
            usePartitions(Collections.singletonList(partitionInfo(10, "east", count)));
            assertThatThrownBy(
                            () ->
                                    access.listScanPartitions(
                                            tableHandle(usersTable()), Optional.empty()))
                    .isInstanceOfSatisfying(
                            TrinoException.class,
                            e ->
                                    assertThat(e.getErrorCode())
                                            .isEqualTo(FLUSS_SPLIT_ERROR.toErrorCode()))
                    .hasMessageContaining("Invalid bucket count");
        }
    }

    @Test
    void testListScanPartitionsRejectsNegativePartitionId() throws Exception {
        usePartitions(Collections.singletonList(partitionInfo(-1, "east", 2)));
        assertThatThrownBy(
                        () ->
                                access.listScanPartitions(
                                        tableHandle(usersTable()), Optional.empty()))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(FLUSS_SPLIT_ERROR.toErrorCode()))
                .hasMessageContaining("Invalid Fluss partition ID");
    }

    @Test
    void testListScanPartitionsRpcFailureTranslation() throws Exception {
        for (RuntimeException failure :
                Arrays.asList(
                        new PartitionNotExistException("expired"),
                        new IllegalStateException("RPC failed"))) {
            useAdmin(
                    new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                        @Override
                        public CompletableFuture<List<PartitionInfo>> listPartitionInfos(
                                TablePath path) {
                            return FutureUtils.completedExceptionally(failure);
                        }
                    });
            assertThatThrownBy(
                            () ->
                                    access.listScanPartitions(
                                            tableHandle(usersTable()), Optional.empty()))
                    .isInstanceOfSatisfying(
                            TrinoException.class,
                            e ->
                                    assertThat(e.getErrorCode())
                                            .isEqualTo(FLUSS_SPLIT_ERROR.toErrorCode()))
                    .hasCause(failure);
        }
    }

    @Test
    void testListScanPartitionsReturnsEmptyList() throws Exception {
        FlussTableHandle handle =
                tableHandle(createTable(tablePath, usersTable().toTableDescriptor()));
        assertThat(access.listScanPartitions(handle, Optional.empty())).isEmpty();
        admin.dropTable(tablePath, false).get(30, TimeUnit.SECONDS);
        assertThatThrownBy(() -> access.listScanPartitions(handle, Optional.empty()))
                .isInstanceOf(TableNotFoundException.class);
        admin.dropDatabase(databaseName("Sales"), false, true).get(30, TimeUnit.SECONDS);
        assertThatThrownBy(() -> access.listScanPartitions(handle, Optional.empty()))
                .isInstanceOf(TableNotFoundException.class);
    }

    private static PartitionInfo partitionInfo(long id, String name, int count) {
        return new PartitionInfo(
                id, ResolvedPartitionSpec.fromPartitionValue("region", name), null, count);
    }

    @Test
    void testUnexpectedFailurePreservesCause() throws Exception {
        RuntimeException failure = new IllegalStateException("RPC failed");
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public CompletableFuture<List<String>> listDatabases() {
                        return FutureUtils.completedExceptionally(failure);
                    }
                });
        assertThatThrownBy(access::listSchemaNames)
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(FLUSS_METADATA_ERROR.toErrorCode()))
                .hasCause(failure);
    }

    @Test
    void testDiscoveryDoesNotHideUnexpectedFailure() throws Exception {
        RuntimeException failure = new IllegalStateException("permission denied");
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public CompletableFuture<TableInfo> getTableInfo(TablePath path) {
                        return FutureUtils.completedExceptionally(failure);
                    }
                });
        assertThatThrownBy(
                        () ->
                                access.findTableInfo(
                                        new ResolvedTableName(
                                                new SchemaTableName(databaseName("sales"), "users"),
                                                databaseName("Sales"),
                                                "Users")))
                .isInstanceOf(TrinoException.class)
                .hasCause(failure);
    }

    private void usePartitions(List<PartitionInfo> partitions) throws Exception {
        // Only malformed server responses bypass the real catalog, which rejects these values.
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public CompletableFuture<List<PartitionInfo>> listPartitionInfos(
                            TablePath path) {
                        return completedFuture(partitions);
                    }
                });
    }
}
