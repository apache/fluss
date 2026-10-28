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
import org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.SchemaUpdate;
import org.apache.fluss.types.DataTypes;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;
import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_FORMAT;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.LAKESTREAM_ENABLED_OPTION_KEY;
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
    void testGetTableDescriptor() throws Exception {
        TablePath tablePath = TablePath.of("get_table_descriptor_db", "get_table_descriptor_table");
        createPaimonTable(
                tablePath,
                org.apache.paimon.schema.Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .build());

        TableDescriptor descriptor = flussPaimonCatalog.getTableDescriptor(tablePath);
        assertThat(descriptor.getSchema().getColumnNames()).containsExactly("id");
        assertThat(descriptor.getTableDistribution()).isPresent();
    }

    @Test
    void testGetTableDescriptorWithNonExistentTable() {
        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.getTableDescriptor(
                                        TablePath.of("missing_db", "missing_table")))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("Table missing_db.missing_table does not exist in Paimon.");
    }

    @Test
    void testGetLatestSnapshotId() throws Exception {
        TablePath tablePath = TablePath.of("get_snapshot_db", "get_snapshot_table");
        assertThatThrownBy(() -> flussPaimonCatalog.getLatestSnapshotId(tablePath))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("Table get_snapshot_db.get_snapshot_table does not exist in Paimon.");

        createPaimonTable(
                tablePath,
                org.apache.paimon.schema.Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .build());

        assertThat(flussPaimonCatalog.getLatestSnapshotId(tablePath)).isEmpty();

        long snapshotId = writeSingleRow(tablePath);

        assertThat(flussPaimonCatalog.getLatestSnapshotId(tablePath)).contains(snapshotId);
    }

    @Test
    void testLatestSnapshotWhenOnlyCompactionSnapshotRemains() throws Exception {
        TablePath tablePath = TablePath.of("expired_snapshot_db", "expired_snapshot_table");
        Schema schema = Schema.newBuilder().column("id", DataTypes.BIGINT()).build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();
        createPaimonTable(
                tablePath,
                org.apache.paimon.schema.Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option(PARTITION_GENERATE_LEGACY_NAME_OPTION_KEY, "false")
                        .build());

        for (int i = 0; i < 5; i++) {
            writeSingleRow(tablePath);
        }
        FileStoreTable table =
                (FileStoreTable)
                        flussPaimonCatalog.getPaimonCatalog().getTable(toPaimon(tablePath));
        new CompactHelper(table, new File(tempWarehouseDir, "expired-compaction"))
                .compactBucket(0)
                .commit();
        long compactSnapshotId = table.latestSnapshot().get().id();
        assertThat(table.latestSnapshot().get().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);

        // Retain only the COMPACT snapshot after deleting the earlier APPEND snapshots.
        for (long snapshotId = 1; snapshotId < compactSnapshotId; snapshotId++) {
            table.snapshotManager().deleteSnapshot(snapshotId);
        }
        table.snapshotManager().invalidateCache();
        assertThat(table.snapshotManager().snapshotExists(compactSnapshotId)).isTrue();
        assertThat(flussPaimonCatalog.getLatestSnapshotId(tablePath)).contains(compactSnapshotId);
        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.createTable(
                                        tablePath,
                                        tableDescriptor,
                                        new TestingLakeCatalogContext(
                                                tableDescriptor, tableDescriptor)))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Fluss and Paimon snapshot IDs are inconsistent");

        // Earlier APPEND snapshots are no longer needed once the COMPACT is registered.
        TestingLakeCatalogContext compactContext =
                new TestingLakeCatalogContext(tableDescriptor, tableDescriptor) {
                    @Override
                    public Optional<Long> getCurrentLakeSnapshotId() {
                        return Optional.of(compactSnapshotId);
                    }
                };
        flussPaimonCatalog.createTable(tablePath, tableDescriptor, compactContext);
    }

    @Test
    void testSnapshotConsistencyAfterCompaction() throws Exception {
        TablePath tablePath = TablePath.of("compact_snapshot_db", "compact_snapshot_table");
        Schema schema = Schema.newBuilder().column("id", DataTypes.BIGINT()).build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();
        createPaimonTable(
                tablePath,
                org.apache.paimon.schema.Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option(PARTITION_GENERATE_LEGACY_NAME_OPTION_KEY, "false")
                        .build());

        long registeredSnapshotId = -1;
        for (int i = 0; i < 5; i++) {
            registeredSnapshotId = writeSingleRow(tablePath);
        }
        long expectedRegisteredSnapshotId = registeredSnapshotId;
        FileStoreTable table =
                (FileStoreTable)
                        flussPaimonCatalog.getPaimonCatalog().getTable(toPaimon(tablePath));
        new CompactHelper(table, new File(tempWarehouseDir, "compaction"))
                .compactBucket(0)
                .commit();
        assertThat(table.latestSnapshot().get().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
        long compactSnapshotId = table.latestSnapshot().get().id();
        assertThat(flussPaimonCatalog.getLatestSnapshotId(tablePath)).contains(compactSnapshotId);

        TestingLakeCatalogContext context =
                new TestingLakeCatalogContext(tableDescriptor, tableDescriptor) {
                    @Override
                    public Optional<Long> getCurrentLakeSnapshotId() {
                        return Optional.of(expectedRegisteredSnapshotId);
                    }
                };
        flussPaimonCatalog.createTable(tablePath, tableDescriptor, context);

        // A registered COMPACT snapshot also remains valid until another data change.
        TestingLakeCatalogContext compactContext =
                new TestingLakeCatalogContext(tableDescriptor, tableDescriptor) {
                    @Override
                    public Optional<Long> getCurrentLakeSnapshotId() {
                        return Optional.of(compactSnapshotId);
                    }
                };
        flussPaimonCatalog.createTable(tablePath, tableDescriptor, compactContext);

        // The registered snapshot must still exist even when later snapshots are only COMPACT.
        table.snapshotManager().deleteSnapshot(expectedRegisteredSnapshotId);
        table.snapshotManager().invalidateCache();
        assertThatThrownBy(
                        () -> flussPaimonCatalog.createTable(tablePath, tableDescriptor, context))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("The registered Paimon snapshot has expired")
                .hasMessageContaining("Fluss snapshot: " + expectedRegisteredSnapshotId)
                .hasMessageContaining("Paimon latest snapshot: " + compactSnapshotId);

        long unregisteredSnapshotId = writeSingleRow(tablePath);
        assertThat(flussPaimonCatalog.getLatestSnapshotId(tablePath))
                .contains(unregisteredSnapshotId);
        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.createTable(
                                        tablePath, tableDescriptor, compactContext))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("changed table data")
                .hasMessageContaining("Fluss snapshot: " + compactSnapshotId)
                .hasMessageContaining("Paimon latest snapshot: " + unregisteredSnapshotId);

        // A later COMPACT must not hide the unregistered APPEND snapshots.
        for (int i = 0; i < 4; i++) {
            writeSingleRow(tablePath);
        }
        new CompactHelper(table, new File(tempWarehouseDir, "second-compaction"))
                .compactBucket(0)
                .commit();
        assertThat(table.latestSnapshot().get().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.createTable(
                                        tablePath, tableDescriptor, compactContext))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("changed table data");

        long missingSnapshotId = table.latestSnapshot().get().id() - 1;
        table.snapshotManager().deleteSnapshot(missingSnapshotId);
        table.snapshotManager().invalidateCache();
        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.createTable(
                                        tablePath, tableDescriptor, compactContext))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Paimon snapshot " + missingSnapshotId + " is missing");
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
                .containsSequence("id", "name", "amount", "address", "new_col");
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
                .containsSequence("id", "name", "amount", "address", "is_direct_play");
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
                .isTrue();
        assertThat(
                        isPaimonSchemaCompatible(
                                schema,
                                createPaimonSchemaWithoutPartitionLegacyName(
                                        Arrays.asList("id", "pt"),
                                        Collections.singletonList("pt"),
                                        "1",
                                        "id")))
                .isTrue();
        assertThat(CoreOptions.IMMUTABLE_OPTIONS).contains(CoreOptions.BUCKET_FUNCTION_TYPE.key());
        org.apache.paimon.schema.Schema schemaWithExplicitDefault =
                createPaimonSchemaBuilder(
                                Arrays.asList("id", "pt"),
                                Collections.singletonList("pt"),
                                "1",
                                "id")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "false")
                        .build();
        assertThat(isPaimonSchemaCompatible(schemaWithExplicitDefault, schema)).isTrue();
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
    void testPaimonSchemaCompatibilityWithVarCharAndVarBinary() {
        org.apache.paimon.schema.Schema existingSchema =
                org.apache.paimon.schema.Schema.newBuilder()
                        .column("name", org.apache.paimon.types.DataTypes.VARCHAR(20).notNull())
                        .column("payload", org.apache.paimon.types.DataTypes.VARBINARY(128))
                        .build();
        org.apache.paimon.schema.Schema convertedSchema =
                org.apache.paimon.schema.Schema.newBuilder()
                        .column("name", org.apache.paimon.types.DataTypes.STRING().notNull())
                        .column("payload", org.apache.paimon.types.DataTypes.BYTES())
                        .build();

        // Fluss loses the declared lengths, but the converted types retain nullability.
        assertThat(isPaimonSchemaCompatible(existingSchema, convertedSchema)).isTrue();

        // Changing nullability in either column must still make the schemas incompatible.
        assertThat(
                        isPaimonSchemaCompatible(
                                existingSchema,
                                org.apache.paimon.schema.Schema.newBuilder()
                                        .column("name", org.apache.paimon.types.DataTypes.STRING())
                                        .column(
                                                "payload",
                                                org.apache.paimon.types.DataTypes.BYTES())
                                        .build()))
                .isFalse();

        assertThat(
                        isPaimonSchemaCompatible(
                                existingSchema,
                                org.apache.paimon.schema.Schema.newBuilder()
                                        .column(
                                                "name",
                                                org.apache.paimon.types.DataTypes.STRING()
                                                        .notNull())
                                        .column(
                                                "payload",
                                                org.apache.paimon.types.DataTypes.BYTES().notNull())
                                        .build()))
                .isFalse();
    }

    @Test
    void testPaimonSchemaCompatibilityWithNestedVarCharAndVarBinary() {
        org.apache.paimon.schema.Schema existingSchema =
                org.apache.paimon.schema.Schema.newBuilder()
                        .column(
                                "names",
                                org.apache.paimon.types.DataTypes.ARRAY(
                                        org.apache.paimon.types.DataTypes.VARCHAR(20).notNull()))
                        .column(
                                "payloads",
                                org.apache.paimon.types.DataTypes.ARRAY(
                                        org.apache.paimon.types.DataTypes.VARBINARY(128)))
                        .column(
                                "attributes",
                                org.apache.paimon.types.DataTypes.MAP(
                                        org.apache.paimon.types.DataTypes.VARCHAR(20),
                                        org.apache.paimon.types.DataTypes.VARBINARY(128)))
                        .column(
                                "person",
                                org.apache.paimon.types.DataTypes.ROW(
                                        org.apache.paimon.types.DataTypes.FIELD(
                                                0,
                                                "name",
                                                org.apache.paimon.types.DataTypes.VARCHAR(20))))
                        .build();
        org.apache.paimon.schema.Schema convertedSchema =
                org.apache.paimon.schema.Schema.newBuilder()
                        .column(
                                "names",
                                org.apache.paimon.types.DataTypes.ARRAY(
                                        org.apache.paimon.types.DataTypes.STRING().notNull()))
                        .column(
                                "payloads",
                                org.apache.paimon.types.DataTypes.ARRAY(
                                        org.apache.paimon.types.DataTypes.BYTES()))
                        .column(
                                "attributes",
                                org.apache.paimon.types.DataTypes.MAP(
                                        org.apache.paimon.types.DataTypes.STRING().notNull(),
                                        org.apache.paimon.types.DataTypes.BYTES()))
                        .column(
                                "person",
                                org.apache.paimon.types.DataTypes.ROW(
                                        org.apache.paimon.types.DataTypes.FIELD(
                                                1,
                                                "name",
                                                org.apache.paimon.types.DataTypes.STRING())))
                        .build();

        // Conversion drops bounded lengths and ROW field ids; Fluss also makes MAP keys non-null.
        assertThat(isPaimonSchemaCompatible(existingSchema, convertedSchema)).isTrue();

        // Nullable changes at an ARRAY element, MAP value, ROW field, or container remain
        // incompatible even when the bounded lengths are normalized.
        DataType[][] mismatchedTypes =
                new DataType[][] {
                    {
                        org.apache.paimon.types.DataTypes.ARRAY(
                                org.apache.paimon.types.DataTypes.VARCHAR(20).notNull()),
                        org.apache.paimon.types.DataTypes.ARRAY(
                                org.apache.paimon.types.DataTypes.STRING())
                    },
                    {
                        org.apache.paimon.types.DataTypes.MAP(
                                org.apache.paimon.types.DataTypes.VARCHAR(20),
                                org.apache.paimon.types.DataTypes.VARBINARY(128)),
                        org.apache.paimon.types.DataTypes.MAP(
                                org.apache.paimon.types.DataTypes.STRING().notNull(),
                                org.apache.paimon.types.DataTypes.BYTES().notNull())
                    },
                    {
                        org.apache.paimon.types.DataTypes.ROW(
                                org.apache.paimon.types.DataTypes.FIELD(
                                        0, "name", org.apache.paimon.types.DataTypes.VARCHAR(20))),
                        org.apache.paimon.types.DataTypes.ROW(
                                org.apache.paimon.types.DataTypes.FIELD(
                                        1,
                                        "name",
                                        org.apache.paimon.types.DataTypes.STRING().notNull()))
                    },
                    {
                        org.apache.paimon.types.DataTypes.ARRAY(
                                        org.apache.paimon.types.DataTypes.VARBINARY(128))
                                .notNull(),
                        org.apache.paimon.types.DataTypes.ARRAY(
                                org.apache.paimon.types.DataTypes.BYTES())
                    }
                };
        for (DataType[] types : mismatchedTypes) {
            assertThat(
                            isPaimonSchemaCompatible(
                                    org.apache.paimon.schema.Schema.newBuilder()
                                            .column("value", types[0])
                                            .build(),
                                    org.apache.paimon.schema.Schema.newBuilder()
                                            .column("value", types[1])
                                            .build()))
                    .isFalse();
        }
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

    @Test
    void testCreateTableSetsLakeStreamEnabledForCleanTable() throws Exception {
        String database = "test_create_lakestream_db";
        String tableName = "test_create_lakestream_table";
        TablePath tablePath = TablePath.of(database, tableName);
        Identifier identifier = Identifier.create(database, tableName);

        // getTableDescriptor sets table.datalake.enabled=true, so the clean table advertises its
        // LakeStream state to Paimon
        flussPaimonCatalog.createTable(
                tablePath, getTableDescriptor(FLUSS_SCHEMA), LAKE_CATALOG_CONTEXT);

        Table table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        assertThat(table.options()).containsEntry(LAKESTREAM_ENABLED_OPTION_KEY, "true");
    }

    @Test
    void testCreateTableWithoutDataLakeEnabledHasNoLakeStreamOption() throws Exception {
        String database = "test_create_no_lakestream_db";
        String tableName = "test_create_no_lakestream_table";
        TablePath tablePath = TablePath.of(database, tableName);
        Identifier identifier = Identifier.create(database, tableName);

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(FLUSS_SCHEMA)
                        .property(TABLE_DATALAKE_ENABLED.key(), "false")
                        .property(TABLE_DATALAKE_FORMAT.key(), "paimon")
                        .property(
                                "table.datalake.paimon.warehouse",
                                tempWarehouseDir.toURI().toString())
                        .distributedBy(3)
                        .build();
        flussPaimonCatalog.createTable(tablePath, tableDescriptor, LAKE_CATALOG_CONTEXT);

        Table table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        assertThat(table.options()).doesNotContainKey(LAKESTREAM_ENABLED_OPTION_KEY);
    }

    @Test
    void testAlterDataLakeEnabledMaintainsLakeStreamOptionForCleanTable() throws Exception {
        String database = "test_alter_lakestream_db";
        String tableName = "test_alter_lakestream_table";
        TablePath tablePath = TablePath.of(database, tableName);
        Identifier identifier = Identifier.create(database, tableName);
        createTable(database, tableName);

        // disable lake acceleration removes lakestream.enabled instead of storing false
        flussPaimonCatalog.alterTable(
                tablePath,
                Collections.singletonList(TableChange.set(TABLE_DATALAKE_ENABLED.key(), "false")),
                LAKE_CATALOG_CONTEXT);
        Table table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        assertThat(table.options()).doesNotContainKey(LAKESTREAM_ENABLED_OPTION_KEY);

        // re-enable lake acceleration adds lakestream.enabled=true again
        flussPaimonCatalog.alterTable(
                tablePath,
                Collections.singletonList(TableChange.set(TABLE_DATALAKE_ENABLED.key(), "true")),
                LAKE_CATALOG_CONTEXT);
        table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        assertThat(table.options()).containsEntry(LAKESTREAM_ENABLED_OPTION_KEY, "true");

        // resetting datalake.enabled is equivalent to disabling acceleration
        flussPaimonCatalog.alterTable(
                tablePath,
                Collections.singletonList(TableChange.reset(TABLE_DATALAKE_ENABLED.key())),
                LAKE_CATALOG_CONTEXT);
        table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        assertThat(table.options()).doesNotContainKey(LAKESTREAM_ENABLED_OPTION_KEY);
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

    @Test
    void testUserFacingAlterTableStillRejectsBucketChange() throws Exception {
        // Fluss's typed bucket-count change is applied, while any attempt to set Paimon's own
        // bucket option directly through a property change keeps being rejected.
        String database = "test_user_bucket_reject_db";
        String tableName = "test_user_bucket_reject_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createFixedBucketTable(database, tableName, 4);

        TableDescriptor fixedBucketDescriptor = fixedBucketTableDescriptor(4);
        TestingLakeCatalogContext matchingContext =
                new TestingLakeCatalogContext(fixedBucketDescriptor, fixedBucketDescriptor);

        List<TableChange> userChanges =
                Collections.singletonList(
                        TableChange.set(
                                "paimon." + org.apache.paimon.CoreOptions.BUCKET.key(), "8"));
        assertThatThrownBy(
                        () ->
                                flussPaimonCatalog.alterTable(
                                        tablePath, userChanges, matchingContext))
                .hasMessageContaining("bucket")
                .hasMessageContaining("cannot be changed");

        flussPaimonCatalog.alterTable(
                tablePath,
                Collections.singletonList(TableChange.modifyBucketCount(8)),
                matchingContext);
        Identifier identifier = Identifier.create(database, tableName);
        Table after = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        assertThat(after.options().get(org.apache.paimon.CoreOptions.BUCKET.key())).isEqualTo("8");
    }

    private long writeSingleRow(TablePath tablePath) throws Exception {
        FileStoreTable table =
                (FileStoreTable)
                        flussPaimonCatalog.getPaimonCatalog().getTable(toPaimon(tablePath));
        BatchWriteBuilder writeBuilder =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"))
                        .newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1L));
            commit.commit(write.prepareCommit());
        }
        return table.latestSnapshot().get().id();
    }

    private TableDescriptor fixedBucketTableDescriptor(int bucketCount) {
        return TableDescriptor.builder()
                .schema(FLUSS_SCHEMA)
                .property(TABLE_DATALAKE_ENABLED.key(), "true")
                .property(TABLE_DATALAKE_FORMAT.key(), "paimon")
                .property("table.datalake.paimon.warehouse", tempWarehouseDir.toURI().toString())
                .distributedBy(bucketCount, "id")
                .build();
    }

    private void createFixedBucketTable(String database, String tableName, int initialBucketCount) {
        TableDescriptor td = fixedBucketTableDescriptor(initialBucketCount);
        flussPaimonCatalog.createTable(
                TablePath.of(database, tableName), td, new TestingLakeCatalogContext(td, td));
    }

    private void createTable(String database, String tableName) {
        TableDescriptor td = getTableDescriptor(FLUSS_SCHEMA);
        TablePath tablePath = TablePath.of(database, tableName);

        flussPaimonCatalog.createTable(tablePath, td, LAKE_CATALOG_CONTEXT);
    }

    private void createPaimonTable(
            TablePath tablePath, org.apache.paimon.schema.Schema paimonSchema) throws Exception {
        flussPaimonCatalog.getPaimonCatalog().createDatabase(tablePath.getDatabaseName(), true);
        flussPaimonCatalog.getPaimonCatalog().createTable(toPaimon(tablePath), paimonSchema, false);
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
