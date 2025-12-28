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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link PaimonLakeCatalog}. */
class PaimonLakeCatalogTest {

    @TempDir private File tempWarehouseDir;

    private PaimonLakeCatalog flussPaimonCatalog;

    @BeforeEach
    public void setUp() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toURI().toString());
        flussPaimonCatalog = new PaimonLakeCatalog(configuration);
    }

    @Test
    void testAlterTableProperties() throws Exception {
        String database = "test_alter_table_properties_db";
        String tableName = "test_alter_table_properties_table";
        TablePath tablePath = TablePath.of(database, tableName);
        Identifier identifier = Identifier.create(database, tableName);
        createTable(database, tableName);
        Table table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);

        // value should be null for key
        assertThat(table.options().get("key")).isEqualTo(null);

        // set the value for key
        flussPaimonCatalog.alterTable(
                tablePath,
                Collections.singletonList(TableChange.set("key", "value")),
                new TestingLakeCatalogContext());

        table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        // we have set the value for key
        assertThat(table.options().get("fluss.key")).isEqualTo("value");

        // reset the value for key
        flussPaimonCatalog.alterTable(
                tablePath,
                Collections.singletonList(TableChange.reset("key")),
                new TestingLakeCatalogContext());

        table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        // we have reset the value for key
        assertThat(table.options().get("fluss.key")).isEqualTo(null);
    }

    @Test
    void alterTablePropertiesWithNonExistentTable() {
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();
        // db & table don't exist
        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.alterTable(
                                        TablePath.of("non_existing_db", "non_existing_table"),
                                        Collections.singletonList(TableChange.set("key", "value")),
                                        context))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("Table non_existing_db.non_existing_table does not exist.");

        String database = "alter_props_db";
        String tableName = "alter_props_table";
        createTable(database, tableName);

        // database exists but table doesn't
        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.alterTable(
                                        TablePath.of(database, "non_existing_table"),
                                        Collections.singletonList(TableChange.set("key", "value")),
                                        context))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("Table alter_props_db.non_existing_table does not exist.");
    }

    @Test
    void testAlterTableAddColumnLastNullable() throws Exception {
        String database = "test_alter_table_add_column_db";
        String tableName = "test_alter_table_add_column_table";
        TablePath tablePath = TablePath.of(database, tableName);
        Identifier identifier = Identifier.create(database, tableName);
        createTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_col",
                                DataTypes.INT(),
                                "new_col comment",
                                TableChange.ColumnPosition.last()));

        flussPaimonCatalog.alterTable(tablePath, changes, new TestingLakeCatalogContext());

        Table table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        assertThat(table.rowType().getFieldNames())
                .containsSequence(
                        "id",
                        "name",
                        "amount",
                        "address",
                        "new_col",
                        "__bucket",
                        "__offset",
                        "__timestamp");
    }

    @Test
    void testAlterTableAddColumnNotLast() {
        String database = "test_alter_table_add_column_not_last_db";
        String tableName = "test_alter_table_add_column_not_last_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_col",
                                DataTypes.INT(),
                                null,
                                TableChange.ColumnPosition.first()));

        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.alterTable(
                                        tablePath, changes, new TestingLakeCatalogContext()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Only support to add column at last for paimon table.");
    }

    @Test
    void testAlterTableAddColumnNotNullable() {
        String database = "test_alter_table_add_column_not_nullable_db";
        String tableName = "test_alter_table_add_column_not_nullable_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_col",
                                DataTypes.INT().copy(false),
                                null,
                                TableChange.ColumnPosition.last()));

        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.alterTable(
                                        tablePath, changes, new TestingLakeCatalogContext()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Only support to add nullable column for paimon table.");
    }

    @Test
    void testAlterTableAddExistingColumn() {
        String database = "test_alter_table_add_existing_column_db";
        String tableName = "test_alter_table_add_existing_column_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "address",
                                DataTypes.STRING(),
                                null,
                                TableChange.ColumnPosition.last()));

        // no exception thrown when adding existing column
        flussPaimonCatalog.alterTable(tablePath, changes, new TestingLakeCatalogContext());

        List<TableChange> changes2 =
                Collections.singletonList(
                        TableChange.addColumn(
                                "address",
                                DataTypes.INT(),
                                null,
                                TableChange.ColumnPosition.last()));

        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.alterTable(
                                        tablePath, changes2, new TestingLakeCatalogContext()))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessage(
                        "Column 'address' already exists but with different type. Existing: STRING, Expected: INT");

        List<TableChange> changes3 =
                Collections.singletonList(
                        TableChange.addColumn(
                                "address",
                                DataTypes.STRING(),
                                "the address comment",
                                TableChange.ColumnPosition.last()));

        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.alterTable(
                                        tablePath, changes3, new TestingLakeCatalogContext()))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessage(
                        "Column address already exists but with different comment. Existing: null, Expected: the address comment");
    }

    private void createTable(String database, String tableName) {
        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("address", DataTypes.STRING())
                        .build();

        TableDescriptor td =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(3) // no bucket key
                        .build();

        TablePath tablePath = TablePath.of(database, tableName);

        flussPaimonCatalog.createTable(tablePath, td, new TestingLakeCatalogContext());
    }
}
