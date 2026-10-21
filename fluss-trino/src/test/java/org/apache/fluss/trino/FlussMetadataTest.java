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
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.concurrent.FutureUtils;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.fluss.trino.TestingFlussMetadata.metadataAccess;
import static org.apache.fluss.trino.TestingFlussMetadata.tableInfo;
import static org.apache.fluss.trino.TestingFlussMetadata.usersTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests Trino metadata contracts using real metadata access and a mocked Fluss Admin. */
final class FlussMetadataTest {
    private static final SchemaTableName USERS = new SchemaTableName("sales", "users");
    private static final TablePath PHYSICAL_USERS = TablePath.of("Sales", "Users");

    private final Admin admin = mock(Admin.class);
    private final FlussMetadata metadata = new FlussMetadata(metadataAccess(admin));
    private final ConnectorSession session = mock(ConnectorSession.class);

    @Test
    void testApplyFilterPreservesSummaryAndExpressionAsResiduals() {
        FlussTableHandle table = unfilteredTable();
        TupleDomain<ColumnHandle> summary = idConstraint(Domain.singleValue(BIGINT, 7L));
        ConnectorExpression expression =
                new Call(
                        BOOLEAN,
                        new FunctionName("custom_predicate"),
                        Collections.singletonList(new Variable("id", BIGINT)));
        Constraint constraint =
                new Constraint(
                        summary,
                        expression,
                        Collections.singletonMap("id", new FlussColumnHandle("id", 0)));

        ConstraintApplicationResult<ConnectorTableHandle> result =
                metadata.applyFilter(session, table, constraint).get();
        FlussTableHandle filtered = (FlussTableHandle) result.getHandle();
        assertThat(filtered.getConstraint()).isEqualTo(summary);
        assertThat(table.getConstraint().isAll()).isTrue();
        assertThat(result.getRemainingFilter()).isEqualTo(summary);
        assertThat(result.getRemainingExpression()).contains(expression);
        assertThat(result.isPrecalculateStatistics()).isFalse();
        verifyNoInteractions(admin);
    }

    @Test
    void testApplyFilterIntersectsConstraintsAndConverges() {
        TupleDomain<ColumnHandle> initial =
                idConstraint(
                        Domain.create(
                                ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 3L)), false));
        FlussTableHandle table =
                (FlussTableHandle)
                        metadata.applyFilter(session, unfilteredTable(), new Constraint(initial))
                                .get()
                                .getHandle();
        TupleDomain<ColumnHandle> incoming =
                idConstraint(Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 10L)), false));
        ConstraintApplicationResult<ConnectorTableHandle> result =
                metadata.applyFilter(session, table, new Constraint(incoming)).get();
        FlussTableHandle filtered = (FlussTableHandle) result.getHandle();
        assertThat(filtered.getConstraint())
                .isEqualTo(
                        idConstraint(
                                Domain.create(
                                        ValueSet.ofRanges(
                                                Range.range(BIGINT, 3L, true, 10L, false)),
                                        false)));
        assertThat(result.getRemainingFilter()).isEqualTo(incoming);
        assertThat(metadata.applyFilter(session, filtered, new Constraint(incoming))).isEmpty();
        assertThat(metadata.applyFilter(session, filtered, new Constraint(initial))).isEmpty();
        assertThat(metadata.applyFilter(session, filtered, Constraint.alwaysTrue())).isEmpty();
        assertThat(metadata.applyFilter(session, unfilteredTable(), Constraint.alwaysTrue()))
                .isEmpty();
    }

    @Test
    void testApplyFilterCombinesDifferentColumns() {
        TupleDomain<ColumnHandle> ids =
                idConstraint(Domain.multipleValues(BIGINT, Arrays.asList(1L, 3L)));
        FlussTableHandle table =
                (FlussTableHandle)
                        metadata.applyFilter(session, unfilteredTable(), new Constraint(ids))
                                .get()
                                .getHandle();
        TupleDomain<ColumnHandle> names =
                TupleDomain.withColumnDomains(
                        Collections.singletonMap(
                                new FlussColumnHandle("name", 1), Domain.onlyNull(VARCHAR)));
        FlussTableHandle filtered =
                (FlussTableHandle)
                        metadata.applyFilter(session, table, new Constraint(names))
                                .get()
                                .getHandle();
        assertThat(filtered.getConstraint()).isEqualTo(ids.intersect(names));
        assertThat(table.getConstraint()).isEqualTo(ids);
    }

    @Test
    void testApplyFilterContradictionsAndNone() {
        TupleDomain<ColumnHandle> nonNull = idConstraint(Domain.notNull(BIGINT));
        FlussTableHandle table =
                (FlussTableHandle)
                        metadata.applyFilter(session, unfilteredTable(), new Constraint(nonNull))
                                .get()
                                .getHandle();
        FlussTableHandle empty =
                (FlussTableHandle)
                        metadata.applyFilter(
                                        session,
                                        table,
                                        new Constraint(idConstraint(Domain.onlyNull(BIGINT))))
                                .get()
                                .getHandle();
        assertThat(empty.getConstraint().isNone()).isTrue();
        assertThat(metadata.applyFilter(session, empty, new Constraint(nonNull))).isEmpty();
        assertThat(metadata.applyFilter(session, empty, Constraint.alwaysFalse())).isEmpty();
        assertThat(
                        metadata.applyFilter(session, unfilteredTable(), Constraint.alwaysFalse())
                                .get()
                                .getHandle())
                .isEqualTo(empty);
    }

    @Test
    void testExpressionOnlyFilterDoesNotChangeHandle() {
        Constraint constraint =
                new Constraint(
                        TupleDomain.all(),
                        new Variable("flag", BOOLEAN),
                        Collections.singletonMap("flag", new FlussColumnHandle("flag", 2)));
        assertThat(metadata.applyFilter(session, unfilteredTable(), constraint)).isEmpty();
    }

    private static FlussTableHandle unfilteredTable() {
        return new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, TupleDomain.all());
    }

    private static TupleDomain<ColumnHandle> idConstraint(Domain domain) {
        return TupleDomain.withColumnDomains(
                Collections.singletonMap(new FlussColumnHandle("id", 0), domain));
    }

    @Test
    void testTableHandlePreservesPhysicalIdentity() {
        givenTables("Users");
        when(admin.getTableInfo(PHYSICAL_USERS)).thenReturn(completedFuture(usersTable()));
        ConnectorTableHandle handle =
                metadata.getTableHandle(session, USERS, Optional.empty(), Optional.empty());
        assertThat(handle)
                .isEqualTo(
                        new FlussTableHandle(
                                "sales", "users", "Sales", "Users", 42, 3, TupleDomain.all()));
        assertThat(metadata.getTableName(session, handle)).isEqualTo(USERS);
    }

    @Test
    void testMissingTableHasNoHandle() {
        givenTables("Users");
        assertThat(
                        metadata.getTableHandle(
                                session,
                                new SchemaTableName("sales", "missing"),
                                Optional.empty(),
                                Optional.empty()))
                .isNull();
        when(admin.getTableInfo(PHYSICAL_USERS))
                .thenReturn(
                        FutureUtils.completedExceptionally(new TableNotExistException("dropped")));
        assertThat(metadata.getTableHandle(session, USERS, Optional.empty(), Optional.empty()))
                .isNull();
    }

    @Test
    void testRejectTableVersioning() {
        ConnectorTableVersion version = mock(ConnectorTableVersion.class);
        assertThatThrownBy(
                        () ->
                                metadata.getTableHandle(
                                        session, USERS, Optional.of(version), Optional.empty()))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("does not support table versioning");
        assertThatThrownBy(
                        () ->
                                metadata.getTableHandle(
                                        session, USERS, Optional.empty(), Optional.of(version)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("does not support table versioning");
    }

    @Test
    void testTableHandleRejectsAmbiguousPhysicalNames() {
        givenTables("Users", "users");
        assertThatThrownBy(
                        () ->
                                metadata.getTableHandle(
                                        session, USERS, Optional.empty(), Optional.empty()))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("Ambiguous Fluss table");
        verify(admin, never()).getTableInfo(PHYSICAL_USERS);
    }

    @Test
    void testTableMetadataAndColumnHandles() {
        when(admin.getTableInfo(PHYSICAL_USERS)).thenReturn(completedFuture(usersTable()));
        FlussTableHandle handle =
                new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, TupleDomain.all());
        ConnectorTableMetadata table = metadata.getTableMetadata(session, handle);
        assertThat(table.getTable()).isEqualTo(USERS);
        assertThat(table.getComment()).contains("Registered users");
        assertThat(table.getColumns())
                .extracting(ColumnMetadata::getName)
                .containsExactly("region", "id", "name");
        assertThat(table.getColumns())
                .extracting(ColumnMetadata::getType)
                .containsExactly(VARCHAR, BIGINT, VARCHAR);
        assertThat(table.getColumns())
                .extracting(ColumnMetadata::isNullable)
                .containsExactly(false, false, true);
        assertThat(table.getColumns().get(1).getComment()).contains("User identifier");
        assertThat(table.getProperties())
                .containsEntry("primary_key", Arrays.asList("id", "region"));

        Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, handle);
        assertThat(columns).containsOnlyKeys("region", "id", "name");
        assertThat(columns.get("id")).isEqualTo(new FlussColumnHandle("ID", 1));
        assertThat(metadata.getColumnMetadata(session, handle, columns.get("id")))
                .isEqualTo(table.getColumns().get(1));
    }

    @Test
    void testRejectInvalidColumnHandle() {
        when(admin.getTableInfo(PHYSICAL_USERS)).thenReturn(completedFuture(usersTable()));
        FlussTableHandle table =
                new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, TupleDomain.all());
        assertThatThrownBy(
                        () ->
                                metadata.getColumnMetadata(
                                        session, table, new FlussColumnHandle("missing", 3)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid column ordinal");
        assertThatThrownBy(
                        () ->
                                metadata.getColumnMetadata(
                                        session, table, new FlussColumnHandle("id", 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match current table schema");
    }

    @Test
    void testRejectAmbiguousColumnNames() {
        TableInfo info =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("ID", DataTypes.INT())
                                                .column("id", DataTypes.INT())
                                                .build())
                                .distributedBy(1)
                                .build());
        when(admin.getTableInfo(PHYSICAL_USERS)).thenReturn(completedFuture(info));
        FlussTableHandle table =
                new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, TupleDomain.all());
        assertThatThrownBy(() -> metadata.getTableMetadata(session, table))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Ambiguous Fluss columns");
        assertThatThrownBy(() -> metadata.getColumnHandles(session, table))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Ambiguous Fluss columns");
    }

    @Test
    void testListTablesIncludesAmbiguousLogicalNamesOnce() {
        givenTables("Users", "Order", "oRder");
        assertThat(metadata.listTables(session, Optional.of("sales")))
                .containsExactly(USERS, new SchemaTableName("sales", "order"));
    }

    @Test
    void testRelationFilterRunsBeforeResolvingAmbiguousNames() {
        givenTables("Users", "Order", "oRder");
        when(admin.getTableInfo(PHYSICAL_USERS)).thenReturn(completedFuture(usersTable()));
        Iterator<RelationColumnsMetadata> columns =
                metadata.streamRelationColumns(
                        session,
                        Optional.of("sales"),
                        names -> {
                            assertThat(names)
                                    .containsExactlyInAnyOrder(
                                            USERS, new SchemaTableName("sales", "order"));
                            return Collections.singleton(USERS);
                        });
        assertThat(columns)
                .toIterable()
                .singleElement()
                .satisfies(
                        relation -> {
                            assertThat(relation.name()).isEqualTo(USERS);
                            assertThat(relation.tableColumns())
                                    .hasValueSatisfying(
                                            fields ->
                                                    assertThat(fields)
                                                            .extracting(ColumnMetadata::getName)
                                                            .containsExactly(
                                                                    "region", "id", "name"));
                            assertThat(relation.redirected()).isFalse();
                        });
        verify(admin, never()).getTableInfo(TablePath.of("Sales", "Order"));
    }

    @Test
    void testSelectedAmbiguousRelationFails() {
        givenTables("Order", "oRder");
        assertThatThrownBy(
                        () ->
                                metadata.streamRelationColumns(
                                        session, Optional.of("sales"), names -> names))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Ambiguous Fluss table");
    }

    @Test
    void testEmptyRelationFilterDoesNotLoadTableMetadata() {
        givenTables("Users");
        assertThat(
                        metadata.streamRelationColumns(
                                session, Optional.of("sales"), names -> Collections.emptySet()))
                .isExhausted();
        assertThat(
                        metadata.streamRelationComments(
                                session, Optional.of("sales"), names -> Collections.emptySet()))
                .isExhausted();
        verify(admin, never()).getTableInfo(PHYSICAL_USERS);
    }

    @Test
    void testRelationDiscoverySkipsDroppedTables() {
        givenTables("Dropped", "Users");
        when(admin.getTableInfo(TablePath.of("Sales", "Dropped")))
                .thenReturn(
                        FutureUtils.completedExceptionally(new TableNotExistException("dropped")));
        when(admin.getTableInfo(PHYSICAL_USERS)).thenReturn(completedFuture(usersTable()));
        assertThat(metadata.streamRelationColumns(session, Optional.of("sales"), names -> names))
                .toIterable()
                .extracting(RelationColumnsMetadata::name)
                .containsExactly(USERS);
        assertThat(metadata.streamRelationComments(session, Optional.of("sales"), names -> names))
                .toIterable()
                .extracting(RelationCommentMetadata::name)
                .containsExactly(USERS);
    }

    @Test
    void testRelationCommentsApplyFilter() {
        givenTables("Users", "Order", "oRder");
        when(admin.getTableInfo(PHYSICAL_USERS)).thenReturn(completedFuture(usersTable()));
        assertThat(
                        metadata.streamRelationComments(
                                session,
                                Optional.of("sales"),
                                names -> Collections.singleton(USERS)))
                .toIterable()
                .containsExactly(
                        RelationCommentMetadata.forRelation(
                                USERS, Optional.of("Registered users")));
    }

    @Test
    void testEmptyCommentsAreAbsent() {
        givenTables("Users");
        when(admin.getTableInfo(PHYSICAL_USERS))
                .thenReturn(
                        completedFuture(
                                tableInfo(
                                        TableDescriptor.builder()
                                                .schema(
                                                        Schema.newBuilder()
                                                                .column("id", DataTypes.INT())
                                                                .build())
                                                .distributedBy(1)
                                                .comment("")
                                                .build())));
        assertThat(metadata.streamRelationComments(session, Optional.of("sales"), names -> names))
                .toIterable()
                .containsExactly(RelationCommentMetadata.forRelation(USERS, Optional.empty()));
    }

    @Test
    void testColumnsSystemTableDiscovery() {
        givenTables("Users");
        when(admin.getTableInfo(PHYSICAL_USERS)).thenReturn(completedFuture(usersTable()));
        SchemaTableName name = new SchemaTableName("sales", "users$columns");
        assertThat(metadata.getSystemTable(session, name))
                .hasValueSatisfying(
                        table -> assertThat(table.getTableMetadata().getTable()).isEqualTo(name));
        assertThat(metadata.getSystemTable(session, USERS)).isEmpty();
        assertThat(metadata.getSystemTable(session, new SchemaTableName("sales", "$columns")))
                .isEmpty();
        assertThat(
                        metadata.getSystemTable(
                                session, new SchemaTableName("sales", "missing$columns")))
                .isEmpty();
    }

    @Test
    void testSystemTableAbsentWhenBaseTableDropped() {
        givenTables("Users");
        when(admin.getTableInfo(PHYSICAL_USERS))
                .thenReturn(
                        FutureUtils.completedExceptionally(new TableNotExistException("dropped")));
        assertThat(metadata.getSystemTable(session, new SchemaTableName("sales", "users$columns")))
                .isEmpty();
    }

    @Test
    void testMissingSchemaHasNoSystemTable() {
        when(admin.listDatabases()).thenReturn(completedFuture(Collections.emptyList()));
        assertThat(
                        metadata.getSystemTable(
                                session, new SchemaTableName("missing", "users$columns")))
                .isEmpty();
    }

    private void givenTables(String... names) {
        when(admin.listDatabases()).thenReturn(completedFuture(Collections.singletonList("Sales")));
        when(admin.listTables("Sales")).thenReturn(completedFuture(Arrays.asList(names)));
    }
}
