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

package org.apache.fluss.lake.hudi;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.lake.hudi.utils.HudiConversions;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link HudiLakeCatalog}. */
class HudiLakeCatalogTest {

    @TempDir private File tempWarehouseDir;

    private HudiLakeCatalog flussHudiLakeCatalog;

    @BeforeEach
    public void setUp() {
        Configuration configuration = new Configuration();
        configuration.setString("catalog.path", tempWarehouseDir.toURI().toString());
        configuration.setString("mode", "dfs");
        this.flussHudiLakeCatalog = new HudiLakeCatalog(configuration);
    }

    /** Verify property prefix rewriting. */
    @Test
    void testPropertyPrefixRewriting() throws TableNotExistException {
        String database = "test_db";
        String tableName = "test_table";

        Schema flussSchema =
                Schema.newBuilder().column("id", DataTypes.BIGINT()).primaryKey("id").build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(3)
                        .property("hudi.precombine.field", "id")
                        .property("table.datalake.freshness", "30s")
                        .build();

        TablePath tablePath = TablePath.of(database, tableName);
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();
        flussHudiLakeCatalog.createTable(tablePath, tableDescriptor, context);

        CatalogBaseTable table =
                flussHudiLakeCatalog
                        .getHudiCatalog()
                        .getTable(HudiConversions.toHudiObjectPath(tablePath));

        // Verify property prefix rewriting
        assertThat(table.getOptions()).containsEntry("precombine.field", "id");
        assertThat(table.getOptions()).containsEntry("fluss.table.datalake.freshness", "30s");
        assertThat(table.getOptions())
                .doesNotContainKeys("hudi.precombine.field", "table.datalake.freshness");
    }

    @Test
    void testCreatePrimaryKeyTable() throws TableNotExistException {
        String database = "test_db";
        String tableName = "pk_table";

        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .withComment("pk_table")
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(4, "id").build();

        TablePath tablePath = TablePath.of(database, tableName);
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();
        flussHudiLakeCatalog.createTable(tablePath, tableDescriptor, context);

        ObjectPath objectPath = HudiConversions.toHudiObjectPath(tablePath);
        CatalogBaseTable table = flussHudiLakeCatalog.getHudiCatalog().getTable(objectPath);

        assertThat(table).isNotNull();

        List<String> primaryKeys = new ArrayList<>();
        primaryKeys.add("id");
        TableSchema expectHudiSchema =
                TableSchema.builder()
                        .field("id", org.apache.flink.table.api.DataTypes.INT().notNull())
                        .field("name", org.apache.flink.table.api.DataTypes.STRING())
                        .field("__bucket", org.apache.flink.table.api.DataTypes.INT())
                        .field("__offset", org.apache.flink.table.api.DataTypes.BIGINT())
                        .field("__timestamp", org.apache.flink.table.api.DataTypes.TIMESTAMP(6))
                        .primaryKey("primaryKey", primaryKeys.toArray(new String[0]))
                        .build();

        assertThat(table.getUnresolvedSchema()).isEqualTo(expectHudiSchema.toSchema());
    }

    @Test
    void testCreateLogTable() throws TableNotExistException {
        String database = "test_db";
        String tableName = "log_table";

        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .build();

        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("hudi.hoodie.datasource.write.recordkey.field", "id");

        TableDescriptor td =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(3) // no bucket key
                        .customProperties(customProperties)
                        .build();

        TablePath tablePath = TablePath.of(database, tableName);
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();
        flussHudiLakeCatalog.createTable(tablePath, td, context);

        ObjectPath objectPath = HudiConversions.toHudiObjectPath(tablePath);
        CatalogBaseTable table = flussHudiLakeCatalog.getHudiCatalog().getTable(objectPath);

        List<String> primaryKeys = new ArrayList<>();
        primaryKeys.add("id");
        TableSchema expectHudiSchema =
                TableSchema.builder()
                        .field("id", org.apache.flink.table.api.DataTypes.BIGINT().notNull())
                        .field("name", org.apache.flink.table.api.DataTypes.STRING())
                        .field("__bucket", org.apache.flink.table.api.DataTypes.INT())
                        .field("__offset", org.apache.flink.table.api.DataTypes.BIGINT())
                        .field("__timestamp", org.apache.flink.table.api.DataTypes.TIMESTAMP(6))
                        .primaryKey("PK_id", primaryKeys.toArray(new String[0]))
                        .build();

        assertThat(table.getUnresolvedSchema()).isEqualTo(expectHudiSchema.toSchema());
    }

    // ------------------------------------------------------------------
    // isHudiSchemaCompatible() tests
    // ------------------------------------------------------------------

    @Test
    void testIsHudiSchemaCompatibleWithSameSchema() {
        // Build two catalog tables with identical schema
        CatalogTable table1 =
                buildTestCatalogTable(
                        new String[] {"id", "name"},
                        new org.apache.flink.table.api.DataTypes[] {
                            org.apache.flink.table.api.DataTypes.INT().notNull(),
                            org.apache.flink.table.api.DataTypes.STRING()
                        });
        CatalogTable table2 =
                buildTestCatalogTable(
                        new String[] {"id", "name"},
                        new org.apache.flink.table.api.DataTypes[] {
                            org.apache.flink.table.api.DataTypes.INT().notNull(),
                            org.apache.flink.table.api.DataTypes.STRING()
                        });

        assertThat(flussHudiLakeCatalog.isHudiSchemaCompatible(table1, table2)).isTrue();
    }

    @Test
    void testIsHudiSchemaCompatibleWithDifferentColumnCount() {
        CatalogTable table1 =
                buildTestCatalogTable(
                        new String[] {"id", "name"},
                        new org.apache.flink.table.api.DataTypes[] {
                            org.apache.flink.table.api.DataTypes.INT().notNull(),
                            org.apache.flink.table.api.DataTypes.STRING()
                        });
        CatalogTable table2 =
                buildTestCatalogTable(
                        new String[] {"id"},
                        new org.apache.flink.table.api.DataTypes[] {
                            org.apache.flink.table.api.DataTypes.INT().notNull()
                        });

        assertThat(flussHudiLakeCatalog.isHudiSchemaCompatible(table1, table2)).isFalse();
    }

    @Test
    void testIsHudiSchemaCompatibleWithDifferentColumnName() {
        CatalogTable table1 =
                buildTestCatalogTable(
                        new String[] {"id", "name"},
                        new org.apache.flink.table.api.DataTypes[] {
                            org.apache.flink.table.api.DataTypes.INT().notNull(),
                            org.apache.flink.table.api.DataTypes.STRING()
                        });
        CatalogTable table2 =
                buildTestCatalogTable(
                        new String[] {"id", "value"},
                        new org.apache.flink.table.api.DataTypes[] {
                            org.apache.flink.table.api.DataTypes.INT().notNull(),
                            org.apache.flink.table.api.DataTypes.STRING()
                        });

        assertThat(flussHudiLakeCatalog.isHudiSchemaCompatible(table1, table2)).isFalse();
    }

    @Test
    void testIsHudiSchemaCompatibleWithDifferentColumnType() {
        CatalogTable table1 =
                buildTestCatalogTable(
                        new String[] {"id", "name"},
                        new org.apache.flink.table.api.DataTypes[] {
                            org.apache.flink.table.api.DataTypes.INT().notNull(),
                            org.apache.flink.table.api.DataTypes.STRING()
                        });
        CatalogTable table2 =
                buildTestCatalogTable(
                        new String[] {"id", "name"},
                        new org.apache.flink.table.api.DataTypes[] {
                            org.apache.flink.table.api.DataTypes.INT().notNull(),
                            org.apache.flink.table.api.DataTypes.BIGINT()
                        });

        assertThat(flussHudiLakeCatalog.isHudiSchemaCompatible(table1, table2)).isFalse();
    }

    // ------------------------------------------------------------------
    // Duplicate table creation idempotency tests
    // ------------------------------------------------------------------

    @Test
    void testCreateDuplicateTableWithCompatibleSchema() throws TableNotExistException {
        String database = "idempotent_db";
        String tableName = "idempotent_table";

        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(4, "id").build();

        TablePath tablePath = TablePath.of(database, tableName);
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();

        // First creation should succeed
        flussHudiLakeCatalog.createTable(tablePath, tableDescriptor, context);

        // Second creation with same schema should not throw
        flussHudiLakeCatalog.createTable(tablePath, tableDescriptor, context);

        // Verify the table still exists and has correct schema
        CatalogBaseTable table =
                flussHudiLakeCatalog
                        .getHudiCatalog()
                        .getTable(HudiConversions.toHudiObjectPath(tablePath));
        assertThat(table).isNotNull();
    }

    @Test
    void testCreateDuplicateTableWithIncompatibleSchema() {
        String database = "incompat_db";
        String tableName = "incompat_table";

        // First: create a table with id(INT) + name(STRING)
        Schema flussSchema1 =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor1 =
                TableDescriptor.builder().schema(flussSchema1).distributedBy(4, "id").build();

        TablePath tablePath = TablePath.of(database, tableName);
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();
        flussHudiLakeCatalog.createTable(tablePath, tableDescriptor1, context);

        // Second: try creating a table with id(INT) + name(BIGINT) - different type
        Schema flussSchema2 =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor2 =
                TableDescriptor.builder().schema(flussSchema2).distributedBy(4, "id").build();

        assertThatThrownBy(
                        () ->
                                flussHudiLakeCatalog.createTable(
                                        tablePath, tableDescriptor2, context))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("not compatible");
    }

    // ------------------------------------------------------------------
    // HUDI_UNSETTABLE_OPTIONS validation tests
    // ------------------------------------------------------------------

    @Test
    void testUnsettableOptionInPropertiesThrowsException() {
        Schema flussSchema =
                Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id").build();

        // Set a protected Hudi option via properties (without hudi. prefix)
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(4, "id")
                        .property("hudi.hoodie.datasource.write.table.type", "COPY_ON_WRITE")
                        .build();

        TablePath tablePath = TablePath.of("test_db", "protected_option_table");
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();

        assertThatThrownBy(
                        () -> flussHudiLakeCatalog.createTable(tablePath, tableDescriptor, context))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("hoodie.datasource.write.table.type")
                .hasMessageContaining("should not be set manually");
    }

    @Test
    void testUnsettableOptionInCustomPropertiesThrowsException() {
        Schema flussSchema =
                Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id").build();

        Map<String, String> customProperties = new HashMap<>();
        // Set a protected Hudi option via customProperties
        customProperties.put("hudi.hoodie.datasource.write.recordkey.field", "id");

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(4, "id")
                        .customProperties(customProperties)
                        .build();

        TablePath tablePath = TablePath.of("test_db", "protected_custom_table");
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();

        assertThatThrownBy(
                        () -> flussHudiLakeCatalog.createTable(tablePath, tableDescriptor, context))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("hoodie.datasource.write.recordkey.field")
                .hasMessageContaining("should not be set manually");
    }

    @Test
    void testNonProtectedHudiOptionPassesValidation() throws TableNotExistException {
        Schema flussSchema =
                Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id").build();

        // Set a non-protected Hudi option (e.g., precombine.field) — should work fine
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(4, "id")
                        .property("hudi.precombine.field", "id")
                        .build();

        TablePath tablePath = TablePath.of("test_db", "non_protected_table");
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();

        // Should not throw
        flussHudiLakeCatalog.createTable(tablePath, tableDescriptor, context);

        CatalogBaseTable table =
                flussHudiLakeCatalog
                        .getHudiCatalog()
                        .getTable(HudiConversions.toHudiObjectPath(tablePath));
        assertThat(table).isNotNull();
        assertThat(table.getOptions()).containsEntry("precombine.field", "id");
    }

    // ------------------------------------------------------------------
    // System column name conflict tests
    // ------------------------------------------------------------------

    @Test
    void testSystemColumnBucketConflictThrowsException() {
        Schema flussSchema =
                Schema.newBuilder()
                        .column("__bucket", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("__bucket")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(4, "__bucket").build();

        TablePath tablePath = TablePath.of("test_db", "bucket_conflict_table");
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();

        assertThatThrownBy(
                        () -> flussHudiLakeCatalog.createTable(tablePath, tableDescriptor, context))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("__bucket")
                .hasMessageContaining("system column");
    }

    @Test
    void testSystemColumnOffsetConflictThrowsException() {
        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("__offset", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(4, "id").build();

        TablePath tablePath = TablePath.of("test_db", "offset_conflict_table");
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();

        assertThatThrownBy(
                        () -> flussHudiLakeCatalog.createTable(tablePath, tableDescriptor, context))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("__offset")
                .hasMessageContaining("system column");
    }

    @Test
    void testSystemColumnTimestampConflictThrowsException() {
        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("__timestamp", DataTypes.TIMESTAMP(6))
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(4, "id").build();

        TablePath tablePath = TablePath.of("test_db", "timestamp_conflict_table");
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();

        assertThatThrownBy(
                        () -> flussHudiLakeCatalog.createTable(tablePath, tableDescriptor, context))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("__timestamp")
                .hasMessageContaining("system column");
    }

    // ------------------------------------------------------------------
    // Helper methods
    // ------------------------------------------------------------------

    private CatalogTable buildTestCatalogTable(
            String[] columnNames, org.apache.flink.table.api.DataTypes[] columnTypes) {
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < columnNames.length; i++) {
            columns.add(Column.physical(columnNames[i], columnTypes[i]));
        }
        ResolvedSchema resolvedSchema = new ResolvedSchema(columns, Collections.emptyList(), null);
        org.apache.flink.table.api.Schema schema =
                org.apache.flink.table.api.Schema.newBuilder()
                        .fromResolvedSchema(resolvedSchema)
                        .build();
        return CatalogTable.of(schema, null, Collections.emptyList(), new HashMap<>());
    }
}
