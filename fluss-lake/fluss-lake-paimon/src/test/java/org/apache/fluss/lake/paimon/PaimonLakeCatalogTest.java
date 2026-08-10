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
import org.apache.fluss.server.coordinator.SchemaUpdate;
import org.apache.fluss.types.DataTypes;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;
import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_FORMAT;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.PARTITION_GENERATE_LEGACY_NAME_OPTION_KEY;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.lake.paimon.utils.PaimonTableValidation.isPaimonSchemaCompatible;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link PaimonLakeCatalog}. */
class PaimonLakeCatalogTest {
    private static final Schema FLUSS_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .column("name", DataTypes.STRING())
                    .column("amount", DataTypes.INT())
                    .column("address", DataTypes.STRING())
                    .build();
    private static final TestingLakeCatalogContext LAKE_CATALOG_CONTEXT =
            new TestingLakeCatalogContext(TableDescriptor.builder().schema(FLUSS_SCHEMA).build());

    @TempDir private File tempWarehouseDir;

    private PaimonLakeCatalog flussPaimonCatalog;

    @BeforeEach
    void setUp() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toURI().toString());
        flussPaimonCatalog = new PaimonLakeCatalog(configuration);
    }

    @AfterEach
    void cleanup() {
        flussPaimonCatalog.close();
        setUp();
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
                LAKE_CATALOG_CONTEXT);

        table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        // we have set the value for key
        assertThat(table.options().get("fluss.key")).isEqualTo("value");

        // reset the value for key
        flussPaimonCatalog.alterTable(
                tablePath,
                Collections.singletonList(TableChange.reset("key")),
                LAKE_CATALOG_CONTEXT);

        table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        // we have reset the value for key
        assertThat(table.options().get("fluss.key")).isEqualTo(null);
    }

    @Test
    void alterTablePropertiesWithNonExistentTable() {
        // db & table don't exist
        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.alterTable(
                                        TablePath.of("non_existing_db", "non_existing_table"),
                                        Collections.singletonList(TableChange.set("key", "value")),
                                        LAKE_CATALOG_CONTEXT))
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
                                        LAKE_CATALOG_CONTEXT))
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

        flussPaimonCatalog.alterTable(tablePath, changes, LAKE_CATALOG_CONTEXT);

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
    void testAlterTableAddColumnIgnoresPaimonCommentsAndOptions() throws Exception {
        String database = "test_alter_table_add_column_ignores_paimon_comments_and_options_db";
        String tableName = "test_alter_table_add_column_ignores_paimon_comments_and_options_table";
        TablePath tablePath = TablePath.of(database, tableName);
        Identifier identifier = Identifier.create(database, tableName);
        createTable(database, tableName);

        flussPaimonCatalog
                .getPaimonCatalog()
                .alterTable(
                        identifier,
                        Arrays.asList(
                                SchemaChange.updateComment(""),
                                SchemaChange.updateColumnComment(
                                        "address", "updated address comment"),
                                SchemaChange.setOption("fluss.key", "value")),
                        false);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "is_direct_play",
                                DataTypes.SMALLINT(),
                                "whether direct play is enabled",
                                TableChange.ColumnPosition.last()));

        flussPaimonCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(FLUSS_SCHEMA, changes));

        Table table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        assertThat(((FileStoreTable) table).schema().toSchema().comment()).isEqualTo("");
        assertThat(table.options().get("fluss.key")).isEqualTo("value");
        assertThat(table.rowType().getFieldNames())
                .containsSequence(
                        "id",
                        "name",
                        "amount",
                        "address",
                        "is_direct_play",
                        "__bucket",
                        "__offset",
                        "__timestamp");
    }

    @Test
    void testPaimonSchemaCompatibilityChecksPhysicalMetadata() {
        org.apache.paimon.schema.Schema schema =
                createPaimonSchema(
                        Arrays.asList("id", "pt"), Collections.singletonList("pt"), "1", "id");

        assertThat(
                        isPaimonSchemaCompatible(
                                schema,
                                createPaimonSchema(
                                        Arrays.asList("id", "pt"),
                                        Collections.singletonList("pt"),
                                        "2",
                                        "id")))
                .isFalse();
        assertThat(
                        isPaimonSchemaCompatible(
                                schema,
                                createPaimonSchema(
                                        Arrays.asList("id", "pt"),
                                        Collections.singletonList("pt"),
                                        "1",
                                        "pt")))
                .isFalse();
        assertThat(
                        isPaimonSchemaCompatible(
                                schema,
                                createPaimonSchema(
                                        Arrays.asList("pt", "id"),
                                        Collections.singletonList("pt"),
                                        "1",
                                        "id")))
                .isFalse();
        assertThat(
                        isPaimonSchemaCompatible(
                                schema,
                                createPaimonSchema(
                                        Arrays.asList("id", "pt"),
                                        Collections.<String>emptyList(),
                                        "1",
                                        "id")))
                .isFalse();
        assertThat(
                        isPaimonSchemaCompatible(
                                schema,
                                createPaimonSchema(
                                        Arrays.asList("id", "pt"),
                                        Collections.singletonList("pt"),
                                        "1",
                                        "id",
                                        Boolean.TRUE.toString())))
                .isFalse();
        assertThat(
                        isPaimonSchemaCompatible(
                                schema,
                                createPaimonSchemaWithoutPartitionLegacyName(
                                        Arrays.asList("id", "pt"),
                                        Collections.singletonList("pt"),
                                        "1",
                                        "id")))
                .isFalse();

        assertThat(CoreOptions.IMMUTABLE_OPTIONS).contains(CoreOptions.BUCKET_FUNCTION_TYPE.key());
        org.apache.paimon.schema.Schema schemaWithDifferentImmutableOption =
                createPaimonSchemaBuilder(
                                Arrays.asList("id", "pt"),
                                Collections.singletonList("pt"),
                                "1",
                                "id")
                        .option(PARTITION_GENERATE_LEGACY_NAME_OPTION_KEY, Boolean.FALSE.toString())
                        .option(CoreOptions.BUCKET_FUNCTION_TYPE.key(), "mod")
                        .build();
        assertThat(isPaimonSchemaCompatible(schema, schemaWithDifferentImmutableOption)).isFalse();
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
                                        tablePath, changes, LAKE_CATALOG_CONTEXT))
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
                                        tablePath, changes, LAKE_CATALOG_CONTEXT))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Only support to add nullable column for paimon table.");
    }

    @Test
    void testAlterTableAddExistingColumns() throws Exception {
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

        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.alterTable(
                                        tablePath,
                                        changes,
                                        getLakeCatalogContext(FLUSS_SCHEMA, changes)))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Column address already exists");

        List<TableChange> changes2 =
                Arrays.asList(
                        TableChange.addColumn(
                                "new_column",
                                DataTypes.INT(),
                                null,
                                TableChange.ColumnPosition.last()),
                        TableChange.addColumn(
                                "new_column2",
                                DataTypes.STRING(),
                                null,
                                TableChange.ColumnPosition.last()));

        // mock add columns to paimon successfully but fail to add columns to fluss.
        flussPaimonCatalog.alterTable(
                tablePath, changes2, getLakeCatalogContext(FLUSS_SCHEMA, changes2));
        List<TableChange> changes3 =
                Arrays.asList(
                        TableChange.addColumn(
                                "new_column",
                                DataTypes.INT(),
                                null,
                                TableChange.ColumnPosition.last()),
                        TableChange.addColumn(
                                "new_column2",
                                DataTypes.INT(),
                                null,
                                TableChange.ColumnPosition.last()));

        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.alterTable(
                                        tablePath,
                                        changes3,
                                        getLakeCatalogContext(FLUSS_SCHEMA, changes3)))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Paimon schema is not compatible with Fluss schema")
                .hasMessageContaining(
                        String.format(
                                "therefore you need to add the diff columns all at once, rather than applying other table changes: %s.",
                                changes3));

        List<TableChange> changes4 =
                Arrays.asList(
                        TableChange.addColumn(
                                "new_column",
                                DataTypes.INT(),
                                null,
                                TableChange.ColumnPosition.last()),
                        TableChange.addColumn(
                                "new_column2",
                                DataTypes.STRING(),
                                "the address comment",
                                TableChange.ColumnPosition.last()));

        // column comments don't affect schema compatibility.
        flussPaimonCatalog.alterTable(
                tablePath, changes4, getLakeCatalogContext(FLUSS_SCHEMA, changes4));

        // no exception thrown only when adding existing column to match fluss and paimon.
        flussPaimonCatalog.alterTable(
                tablePath, changes2, getLakeCatalogContext(FLUSS_SCHEMA, changes2));
    }

    @Test
    void testAlterTableAddColumnWhenPaimonSchemaNotMatch() throws Exception {
        // this rarely happens only when new fluss lake table with an existed paimon table or use
        // alter table in paimon side directly.
        String database = "test_alter_table_add_column_fluss_wider";
        String tableName = "test_alter_table_add_column_fluss_wider";
        createTable(database, tableName);
        TablePath tablePath = TablePath.of(database, tableName);
        org.apache.paimon.schema.Schema paimonSchema =
                ((FileStoreTable)
                                flussPaimonCatalog.getPaimonCatalog().getTable(toPaimon(tablePath)))
                        .schema()
                        .toSchema();

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_col",
                                DataTypes.INT(),
                                "new_col comment",
                                TableChange.ColumnPosition.last()));

        // test column number mismatch.
        Schema widerFlussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("address", DataTypes.STRING())
                        .column("phone", DataTypes.INT())
                        .build();
        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.alterTable(
                                        tablePath,
                                        changes,
                                        getLakeCatalogContext(widerFlussSchema, changes)))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Paimon schema is not compatible with Fluss schema")
                .hasMessageContaining(
                        String.format(
                                "therefore you need to add the diff columns all at once, rather than applying other table changes: %s.",
                                changes));

        // test column order mismatch.
        Schema disorderflussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("amount", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("address", DataTypes.STRING())
                        .build();
        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.alterTable(
                                        tablePath,
                                        changes,
                                        getLakeCatalogContext(disorderflussSchema, changes)))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Paimon schema is not compatible with Fluss schema")
                .hasMessageContaining(
                        String.format(
                                "therefore you need to add the diff columns all at once, rather than applying other table changes: %s.",
                                changes));
    }

    private org.apache.paimon.schema.Schema createPaimonSchema(
            List<String> primaryKeys, List<String> partitionKeys, String bucket, String bucketKey) {
        return createPaimonSchema(
                primaryKeys, partitionKeys, bucket, bucketKey, Boolean.FALSE.toString());
    }

    private org.apache.paimon.schema.Schema createPaimonSchema(
            List<String> primaryKeys,
            List<String> partitionKeys,
            String bucket,
            String bucketKey,
            String partitionLegacyName) {
        return createPaimonSchemaBuilder(primaryKeys, partitionKeys, bucket, bucketKey)
                .option(PARTITION_GENERATE_LEGACY_NAME_OPTION_KEY, partitionLegacyName)
                .build();
    }

    private org.apache.paimon.schema.Schema createPaimonSchemaWithoutPartitionLegacyName(
            List<String> primaryKeys, List<String> partitionKeys, String bucket, String bucketKey) {
        return createPaimonSchemaBuilder(primaryKeys, partitionKeys, bucket, bucketKey).build();
    }

    private org.apache.paimon.schema.Schema.Builder createPaimonSchemaBuilder(
            List<String> primaryKeys, List<String> partitionKeys, String bucket, String bucketKey) {
        return org.apache.paimon.schema.Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.INT().notNull())
                .column("pt", org.apache.paimon.types.DataTypes.STRING().notNull())
                .primaryKey(primaryKeys.toArray(new String[0]))
                .partitionKeys(partitionKeys.toArray(new String[0]))
                .option(CoreOptions.BUCKET.key(), bucket)
                .option(CoreOptions.BUCKET_KEY.key(), bucketKey);
    }

    private void createTable(String database, String tableName) {
        TableDescriptor td = getTableDescriptor(FLUSS_SCHEMA);
        TablePath tablePath = TablePath.of(database, tableName);

        flussPaimonCatalog.createTable(tablePath, td, LAKE_CATALOG_CONTEXT);
    }

    private TestingLakeCatalogContext getLakeCatalogContext(
            Schema schema, List<TableChange> schemaChanges) {
        Schema expectedSchema = SchemaUpdate.applySchemaChanges(schema, schemaChanges);
        return new TestingLakeCatalogContext(
                getTableDescriptor(schema), getTableDescriptor(expectedSchema));
    }

    private TableDescriptor getTableDescriptor(Schema schema) {
        return TableDescriptor.builder()
                .schema(schema)
                .property(TABLE_DATALAKE_ENABLED.key(), "true")
                .property(TABLE_DATALAKE_FORMAT.key(), "paimon")
                .property("table.datalake.paimon.warehouse", tempWarehouseDir.toURI().toString())
                .distributedBy(3) // no bucket key
                .build();
    }
}
