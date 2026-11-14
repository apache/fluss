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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.PointerType;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.apache.fluss.trino.TestingFlussMetadata.usersTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests Trino metadata contracts against an embedded Fluss cluster. */
final class FlussMetadataTest extends FlussTrinoTestBase {
    private static final SchemaTableName USERS = new SchemaTableName("sales", "users");
    private static final TablePath PHYSICAL_USERS = TablePath.of("Sales", "Users");

    private FlussMetadata metadata;
    private final ConnectorSession session = SESSION;

    @BeforeEach
    void setUpMetadata() {
        metadata = new FlussMetadata(access);
    }

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
    void testTableHandlePreservesPhysicalIdentity() throws Exception {
        givenTables("Users");
        ConnectorTableHandle handle =
                metadata.getTableHandle(session, USERS, Optional.empty(), Optional.empty());
        assertThat(handle).isEqualTo(tableHandle(admin.getTableInfo(PHYSICAL_USERS).get()));
        assertThat(metadata.getTableName(session, handle)).isEqualTo(USERS);
    }

    @Test
    void testMissingTableHasNoHandle() throws Exception {
        givenTables("Users");
        assertThat(
                        metadata.getTableHandle(
                                session,
                                new SchemaTableName("sales", "missing"),
                                Optional.empty(),
                                Optional.empty()))
                .isNull();
        admin.dropTable(PHYSICAL_USERS, false).get();
        assertThat(metadata.getTableHandle(session, USERS, Optional.empty(), Optional.empty()))
                .isNull();
    }

    @Test
    void testRejectTableVersioning() {
        ConnectorTableVersion version =
                new ConnectorTableVersion(PointerType.TARGET_ID, BIGINT, 1L);
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
    void testTableHandleRejectsAmbiguousPhysicalNames() throws Exception {
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
    }

    @Test
    void testTableMetadataAndColumnHandles() throws Exception {
        FlussTableHandle handle =
                tableHandle(createTable(PHYSICAL_USERS, usersTable().toTableDescriptor()));
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
    void testRejectInvalidColumnHandle() throws Exception {
        FlussTableHandle table =
                tableHandle(createTable(PHYSICAL_USERS, usersTable().toTableDescriptor()));
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
    void testRejectAmbiguousColumnNames() throws Exception {
        TableInfo info =
                createTable(
                        PHYSICAL_USERS,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("ID", DataTypes.INT())
                                                .column("id", DataTypes.INT())
                                                .build())
                                .distributedBy(1)
                                .build());
        FlussTableHandle table = tableHandle(info);
        assertThatThrownBy(() -> metadata.getTableMetadata(session, table))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Ambiguous Fluss columns");
        assertThatThrownBy(() -> metadata.getColumnHandles(session, table))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Ambiguous Fluss columns");
    }

    @Test
    void testListTablesIncludesAmbiguousLogicalNamesOnce() throws Exception {
        givenTables("Users", "Order", "oRder");
        assertThat(metadata.listTables(session, Optional.of("sales")))
                .containsExactlyInAnyOrder(USERS, new SchemaTableName("sales", "order"));
    }

    @Test
    void testRelationFilterRunsBeforeResolvingAmbiguousNames() throws Exception {
        givenTables("Users", "Order", "oRder");
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
    }

    @Test
    void testSelectedAmbiguousRelationFails() throws Exception {
        givenTables("Order", "oRder");
        assertThatThrownBy(
                        () ->
                                metadata.streamRelationColumns(
                                        session, Optional.of("sales"), names -> names))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Ambiguous Fluss table");
    }

    @Test
    void testEmptyRelationFilterSkipsAmbiguousTables() throws Exception {
        givenTables("Users", "users");
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public CompletableFuture<TableInfo> getTableInfo(TablePath path) {
                        throw new AssertionError("Filtered relations must not load table metadata");
                    }
                });
        metadata = new FlussMetadata(access);
        assertThat(
                        metadata.streamRelationColumns(
                                session, Optional.of("sales"), names -> Collections.emptySet()))
                .isExhausted();
        assertThat(
                        metadata.streamRelationComments(
                                session, Optional.of("sales"), names -> Collections.emptySet()))
                .isExhausted();
    }

    @Test
    void testRelationDiscoverySkipsDroppedTables() throws Exception {
        givenTables("Dropped", "Users");
        TablePath dropped = TablePath.of("Sales", "Dropped");
        assertThat(
                        metadata.streamRelationColumns(
                                session,
                                Optional.of("sales"),
                                names -> {
                                    assertThat(names)
                                            .contains(new SchemaTableName("sales", "dropped"));
                                    admin.dropTable(dropped, false).join();
                                    return names;
                                }))
                .toIterable()
                .extracting(RelationColumnsMetadata::name)
                .containsExactly(USERS);
        createTable(dropped, usersTable().toTableDescriptor());
        assertThat(
                        metadata.streamRelationComments(
                                session,
                                Optional.of("sales"),
                                names -> {
                                    assertThat(names)
                                            .contains(new SchemaTableName("sales", "dropped"));
                                    admin.dropTable(dropped, false).join();
                                    return names;
                                }))
                .toIterable()
                .extracting(RelationCommentMetadata::name)
                .containsExactly(USERS);
    }

    @Test
    void testRelationCommentsApplyFilter() throws Exception {
        givenTables("Users", "Order", "oRder");
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
    void testEmptyCommentsAreAbsent() throws Exception {
        createTable(
                PHYSICAL_USERS,
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .comment("")
                        .build());
        assertThat(metadata.streamRelationComments(session, Optional.of("sales"), names -> names))
                .toIterable()
                .containsExactly(RelationCommentMetadata.forRelation(USERS, Optional.empty()));
    }

    @Test
    void testColumnsSystemTableDiscovery() throws Exception {
        givenTables("Users");
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
    void testSystemTableAbsentWhenBaseTableDropped() throws Exception {
        givenTables("Users");
        admin.dropTable(PHYSICAL_USERS, false).get();
        assertThat(metadata.getSystemTable(session, new SchemaTableName("sales", "users$columns")))
                .isEmpty();
    }

    @Test
    void testMissingSchemaHasNoSystemTable() {
        assertThat(
                        metadata.getSystemTable(
                                session, new SchemaTableName("missing", "users$columns")))
                .isEmpty();
    }

    private void givenTables(String... names) throws Exception {
        for (String name : names) {
            createTable(TablePath.of("Sales", name), usersTable().toTableDescriptor());
        }
    }
}
