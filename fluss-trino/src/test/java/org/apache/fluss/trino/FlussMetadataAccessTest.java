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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.concurrent.FutureUtils;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.fluss.trino.TestingFlussMetadata.metadataAccess;
import static org.apache.fluss.trino.TestingFlussMetadata.usersTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests physical name resolution and catalog failure translation at the Admin boundary. */
final class FlussMetadataAccessTest {
    private final Admin admin = mock(Admin.class);
    private final FlussMetadataAccess access = metadataAccess(admin);

    @Test
    void testSchemaNamesAreCanonicalAndDistinct() {
        when(admin.listDatabases())
                .thenReturn(completedFuture(Arrays.asList("Sales", "sales", "Inventory")));
        assertThat(access.listSchemaNames()).containsExactlyInAnyOrder("sales", "inventory");
        assertThat(access.resolveSchema("INVENTORY"))
                .hasValueSatisfying(
                        schema -> {
                            assertThat(schema.getTrinoName()).isEqualTo("inventory");
                            assertThat(schema.getFlussName()).isEqualTo("Inventory");
                        });
    }

    @Test
    void testAmbiguousSchema() {
        when(admin.listDatabases()).thenReturn(completedFuture(Arrays.asList("sales", "Sales")));
        assertThatThrownBy(() -> access.resolveSchema("sales"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Ambiguous Fluss database 'sales': Sales, sales");
        assertThatThrownBy(() -> access.listTableIndexes(Optional.empty()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Ambiguous Fluss database");
    }

    @Test
    void testResolvePhysicalTableName() {
        when(admin.listDatabases()).thenReturn(completedFuture(Collections.singletonList("Sales")));
        when(admin.listTables("Sales"))
                .thenReturn(completedFuture(Collections.singletonList("Users")));
        assertThat(access.resolveTable(new SchemaTableName("sales", "users")))
                .hasValueSatisfying(
                        table -> {
                            assertThat(table.getTrinoName())
                                    .isEqualTo(new SchemaTableName("sales", "users"));
                            assertThat(table.getFlussDatabaseName()).isEqualTo("Sales");
                            assertThat(table.getFlussTableName()).isEqualTo("Users");
                        });
        verify(admin).listTables("Sales");
    }

    @Test
    void testCatalogWideListingResolvesEachPhysicalDatabase() {
        when(admin.listDatabases())
                .thenReturn(completedFuture(Arrays.asList("Sales", "Inventory")));
        when(admin.listTables("Sales"))
                .thenReturn(completedFuture(Collections.singletonList("Users")));
        when(admin.listTables("Inventory"))
                .thenReturn(completedFuture(Collections.singletonList("Products")));
        assertThat(access.listTableIndexes(Optional.empty()))
                .flatExtracting(FlussMetadataAccess.TableNameIndex::listTableNames)
                .containsExactly(
                        new SchemaTableName("sales", "users"),
                        new SchemaTableName("inventory", "products"));
    }

    @Test
    void testTargetedListingIgnoresUnrelatedSchemaAmbiguity() {
        when(admin.listDatabases())
                .thenReturn(completedFuture(Arrays.asList("Sales", "sales", "Inventory")));
        when(admin.listTables("Inventory"))
                .thenReturn(completedFuture(Collections.singletonList("Products")));
        assertThat(access.listTableIndexes(Optional.of("inventory")))
                .flatExtracting(FlussMetadataAccess.TableNameIndex::listTableNames)
                .containsExactly(new SchemaTableName("inventory", "products"));
    }

    @Test
    void testMissingSchema() {
        when(admin.listDatabases()).thenReturn(completedFuture(Collections.emptyList()));
        assertThat(access.resolveSchema("missing")).isEmpty();
        assertThat(access.resolveTable(new SchemaTableName("missing", "users"))).isEmpty();
        assertThat(access.listTableIndexes(Optional.of("missing"))).isEmpty();
    }

    @Test
    void testTableIndexResolvesOnlyRequestedNames() {
        when(admin.listTables("Sales"))
                .thenReturn(completedFuture(Arrays.asList("Users", "Order", "oRder")));
        FlussMetadataAccess.TableNameIndex index =
                access.indexTables(new ResolvedSchemaName("sales", "Sales"));
        assertThat(index.listTableNames())
                .containsExactly(
                        new SchemaTableName("sales", "users"),
                        new SchemaTableName("sales", "order"));
        assertThat(index.resolveTable("USERS"))
                .hasValueSatisfying(
                        table -> assertThat(table.getFlussTableName()).isEqualTo("Users"));
        assertThat(index.resolveTable("missing")).isEmpty();
        assertThatThrownBy(() -> index.resolveTable("order"))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Ambiguous Fluss table 'order': Order, oRder");
        verify(admin, times(1)).listTables("Sales");
    }

    @Test
    void testDatabaseDroppedBeforeListingTables() {
        when(admin.listTables("Sales"))
                .thenReturn(
                        FutureUtils.completedExceptionally(
                                new DatabaseNotExistException("dropped")));
        assertThat(access.indexTables(new ResolvedSchemaName("sales", "Sales")).listTableNames())
                .isEmpty();
    }

    @Test
    void testDiscoverySkipsDroppedTableOrDatabase() {
        TablePath path = TablePath.of("Sales", "Users");
        when(admin.getTableInfo(path))
                .thenReturn(
                        FutureUtils.completedExceptionally(new TableNotExistException("dropped")),
                        FutureUtils.completedExceptionally(
                                new DatabaseNotExistException("dropped")));
        ResolvedTableName table =
                new ResolvedTableName(new SchemaTableName("sales", "users"), "Sales", "Users");
        assertThat(access.findTableInfo(table)).isEmpty();
        assertThat(access.findTableInfo(table)).isEmpty();
    }

    @Test
    void testExistingHandleReportsDroppedTable() {
        TableNotExistException failure = new TableNotExistException("dropped");
        when(admin.getTableInfo(TablePath.of("Sales", "Users")))
                .thenReturn(FutureUtils.completedExceptionally(failure));
        assertThatThrownBy(
                        () ->
                                access.getTableInfo(
                                        new FlussTableHandle(
                                                "sales", "users", "Sales", "Users", 42, 3)))
                .isInstanceOf(TableNotFoundException.class)
                .hasCause(failure)
                .hasMessageContaining("sales.users");
    }

    @Test
    void testValidateTableAndSchemaIdentity() {
        TableInfo info = usersTable();
        when(admin.getTableInfo(TablePath.of("Sales", "Users"))).thenReturn(completedFuture(info));
        assertThat(
                        access.getTableInfo(
                                new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3)))
                .isSameAs(info);
        assertThatThrownBy(
                        () ->
                                access.getTableInfo(
                                        new FlussTableHandle(
                                                "sales", "users", "Sales", "Users", 41, 3)))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("changed during query planning");
        assertThatThrownBy(
                        () ->
                                access.getTableInfo(
                                        new FlussTableHandle(
                                                "sales", "users", "Sales", "Users", 42, 2)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("changed during query planning");
    }

    @Test
    void testUnexpectedFailurePreservesCause() {
        RuntimeException failure = new RuntimeException("RPC failed");
        when(admin.listDatabases()).thenReturn(FutureUtils.completedExceptionally(failure));
        assertThatThrownBy(access::listSchemaNames)
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        exception ->
                                assertThat(exception.getErrorCode())
                                        .isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasCause(failure);
    }

    @Test
    void testDiscoveryDoesNotHideUnexpectedFailure() {
        RuntimeException failure = new RuntimeException("permission denied");
        when(admin.getTableInfo(TablePath.of("Sales", "Users")))
                .thenReturn(FutureUtils.completedExceptionally(failure));
        assertThatThrownBy(
                        () ->
                                access.findTableInfo(
                                        new ResolvedTableName(
                                                new SchemaTableName("sales", "users"),
                                                "Sales",
                                                "Users")))
                .isInstanceOf(TrinoException.class)
                .hasCause(failure);
    }
}
