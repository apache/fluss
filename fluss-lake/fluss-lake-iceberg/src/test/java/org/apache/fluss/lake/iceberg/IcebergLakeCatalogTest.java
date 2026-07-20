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

package org.apache.fluss.lake.iceberg;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.SchemaUpdate;
import org.apache.fluss.types.DataTypes;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Unit test for {@link IcebergLakeCatalog}. */
class IcebergLakeCatalogTest {

    private static final Schema FLUSS_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .column("name", DataTypes.STRING())
                    .column("amount", DataTypes.INT())
                    .column("address", DataTypes.STRING())
                    .build();

    @TempDir private File tempWarehouseDir;

    private IcebergLakeCatalog flussIcebergCatalog;

    @BeforeEach
    void setupCatalog() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toURI().toString());
        configuration.setString("catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog");
        configuration.setString("name", "fluss_test_catalog");

        this.flussIcebergCatalog = new IcebergLakeCatalog(configuration);
    }

    /** Verify property prefix rewriting. */
    @Test
    void testPropertyPrefixRewriting() {
        String database = "test_db";
        String tableName = "test_table";

        Schema flussSchema = Schema.newBuilder().column("id", DataTypes.BIGINT()).build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(3)
                        .property("iceberg.commit.retry.num-retries", "5")
                        .property("table.datalake.freshness", "30s")
                        .build();

        TablePath tablePath = TablePath.of(database, tableName);
        flussIcebergCatalog.createTable(
                tablePath, tableDescriptor, new TestingLakeCatalogContext());

        Table created =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));

        // Verify property prefix rewriting
        assertThat(created.properties()).containsEntry("commit.retry.num-retries", "5");
        assertThat(created.properties()).containsEntry("fluss.table.datalake.freshness", "30s");
        assertThat(created.properties())
                .doesNotContainKeys("iceberg.commit.retry.num-retries", "table.datalake.freshness");
    }

    @Test
    void testCreatePrimaryKeyTable() {
        String database = "test_db";
        String tableName = "simple_table";

        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .withComment("field name")
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(4, "id").build();

        TablePath tablePath = TablePath.of(database, tableName);

        flussIcebergCatalog.createTable(
                tablePath, tableDescriptor, new TestingLakeCatalogContext());

        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        Table createdTable = flussIcebergCatalog.getIcebergCatalog().loadTable(tableId);

        assertThat(createdTable).isNotNull();
        assertThat(createdTable.name()).isEqualTo("fluss_test_catalog.test_db.simple_table");

        org.apache.iceberg.Schema expectIcebergSchema =
                new org.apache.iceberg.Schema(
                        Arrays.asList(
                                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                                Types.NestedField.optional(
                                        2, "name", Types.StringType.get(), "field name"),
                                Types.NestedField.required(
                                        3, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                                Types.NestedField.required(
                                        4, OFFSET_COLUMN_NAME, Types.LongType.get()),
                                Types.NestedField.required(
                                        5, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone())),
                        Collections.singleton(1));

        assertThat(createdTable.schema().toString()).isEqualTo(expectIcebergSchema.toString());
        // verify partition spec
        assertThat(createdTable.spec().fields()).hasSize(1);
        PartitionField partitionField = createdTable.spec().fields().get(0);
        assertThat(partitionField.name()).isEqualTo("id_bucket");
        assertThat(partitionField.transform().toString()).isEqualTo("bucket[4]");
        assertThat(partitionField.sourceId()).isEqualTo(1);
    }

    @Test
    void testCreatePartitionedPrimaryKeyTable() {
        String database = "test_db";
        String tableName = "pk_table";

        Schema flussSchema =
                Schema.newBuilder()
                        .column("dt", DataTypes.STRING())
                        .column("user_id", DataTypes.BIGINT())
                        .column("shop_id", DataTypes.BIGINT())
                        .column("num_orders", DataTypes.INT())
                        .column("total_amount", DataTypes.INT().copy(false))
                        .primaryKey("dt", "user_id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(10)
                        .partitionedBy("dt")
                        .property("iceberg.write.format.default", "orc")
                        .property("fluss_k1", "fluss_v1")
                        .build();

        TablePath tablePath = TablePath.of(database, tableName);

        flussIcebergCatalog.createTable(
                tablePath, tableDescriptor, new TestingLakeCatalogContext());

        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        Table createdTable = flussIcebergCatalog.getIcebergCatalog().loadTable(tableId);

        Set<Integer> identifierFieldIds = new HashSet<>();
        identifierFieldIds.add(1);
        identifierFieldIds.add(2);
        org.apache.iceberg.Schema expectIcebergSchema =
                new org.apache.iceberg.Schema(
                        Arrays.asList(
                                Types.NestedField.required(1, "dt", Types.StringType.get()),
                                Types.NestedField.required(2, "user_id", Types.LongType.get()),
                                Types.NestedField.optional(3, "shop_id", Types.LongType.get()),
                                Types.NestedField.optional(
                                        4, "num_orders", Types.IntegerType.get()),
                                Types.NestedField.required(
                                        5, "total_amount", Types.IntegerType.get()),
                                Types.NestedField.required(
                                        6, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                                Types.NestedField.required(
                                        7, OFFSET_COLUMN_NAME, Types.LongType.get()),
                                Types.NestedField.required(
                                        8, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone())),
                        identifierFieldIds);
        assertThat(createdTable.schema().toString()).isEqualTo(expectIcebergSchema.toString());

        // Verify partition spec
        assertThat(createdTable.spec().fields()).hasSize(2);
        // first should be partitioned by the fluss partition key
        PartitionField partitionField1 = createdTable.spec().fields().get(0);
        assertThat(partitionField1.name()).isEqualTo("dt");
        assertThat(partitionField1.transform().toString()).isEqualTo("identity");
        assertThat(partitionField1.sourceId()).isEqualTo(1);

        // the second should be partitioned by primary key
        PartitionField partitionField2 = createdTable.spec().fields().get(1);
        assertThat(partitionField2.name()).isEqualTo("user_id_bucket");
        assertThat(partitionField2.transform().toString()).isEqualTo("bucket[10]");
        assertThat(partitionField2.sourceId()).isEqualTo(2);

        // Verify sort order
        assertThat(createdTable.sortOrder().fields()).hasSize(1);
        SortField sortField = createdTable.sortOrder().fields().get(0);
        assertThat(sortField.sourceId())
                .isEqualTo(createdTable.schema().findField(OFFSET_COLUMN_NAME).fieldId());
        assertThat(sortField.direction()).isEqualTo(SortDirection.ASC);

        // Verify  table properties
        assertThat(createdTable.properties())
                .containsEntry("write.merge.mode", "merge-on-read")
                .containsEntry("write.delete.mode", "merge-on-read")
                .containsEntry("write.update.mode", "merge-on-read")
                .containsEntry("fluss.fluss_k1", "fluss_v1")
                .containsEntry("write.format.default", "orc");
    }

    @Test
    void rejectsPrimaryKeyTableWithMultipleBucketKeys() {
        String database = "test_db";
        String tableName = "multi_bucket_pk_table";

        Schema flussSchema =
                Schema.newBuilder()
                        .column("user_id", DataTypes.BIGINT())
                        .column("shop_id", DataTypes.BIGINT())
                        .column("order_id", DataTypes.BIGINT())
                        .column("amount", DataTypes.DOUBLE())
                        .primaryKey("user_id", "shop_id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(4, "user_id", "shop_id") // Multiple bucket keys
                        .build();

        TablePath tablePath = TablePath.of(database, tableName);

        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.createTable(
                                        tablePath,
                                        tableDescriptor,
                                        new TestingLakeCatalogContext()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Only one bucket key is supported for Iceberg");
    }

    @Test
    void testCreateLogTable() {
        String database = "test_db";
        String tableName = "log_table";

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
        flussIcebergCatalog.createTable(tablePath, td, new TestingLakeCatalogContext());

        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        Table createdTable = flussIcebergCatalog.getIcebergCatalog().loadTable(tableId);

        org.apache.iceberg.Schema expectIcebergSchema =
                new org.apache.iceberg.Schema(
                        Arrays.asList(
                                Types.NestedField.optional(1, "id", Types.LongType.get()),
                                Types.NestedField.optional(2, "name", Types.StringType.get()),
                                Types.NestedField.optional(3, "amount", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "address", Types.StringType.get()),
                                Types.NestedField.required(
                                        5, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                                Types.NestedField.required(
                                        6, OFFSET_COLUMN_NAME, Types.LongType.get()),
                                Types.NestedField.required(
                                        7, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone())));

        // Verify iceberg table schema
        assertThat(createdTable.schema().toString()).isEqualTo(expectIcebergSchema.toString());

        // Verify partition field and transform
        assertThat(createdTable.spec().fields()).hasSize(1);
        PartitionField partitionField = createdTable.spec().fields().get(0);
        assertThat(partitionField.name()).isEqualTo(BUCKET_COLUMN_NAME);
        assertThat(partitionField.transform().toString()).isEqualTo("identity");

        // Verify sort field and order
        assertThat(createdTable.sortOrder().fields()).hasSize(1);
        SortField sortField = createdTable.sortOrder().fields().get(0);
        assertThat(sortField.sourceId())
                .isEqualTo(createdTable.schema().findField(OFFSET_COLUMN_NAME).fieldId());
        assertThat(sortField.direction()).isEqualTo(SortDirection.ASC);
    }

    @Test
    void testCreatePartitionedLogTable() {
        String database = "test_db";
        String tableName = "partitioned_log_table";

        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("order_type", DataTypes.STRING())
                        .build();

        TableDescriptor td =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(3)
                        .partitionedBy("order_type")
                        .build();

        TablePath path = TablePath.of(database, tableName);
        flussIcebergCatalog.createTable(path, td, new TestingLakeCatalogContext());

        Table createdTable =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));

        org.apache.iceberg.Schema expectIcebergSchema =
                new org.apache.iceberg.Schema(
                        Arrays.asList(
                                Types.NestedField.optional(1, "id", Types.LongType.get()),
                                Types.NestedField.optional(2, "name", Types.StringType.get()),
                                Types.NestedField.optional(3, "amount", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "order_type", Types.StringType.get()),
                                Types.NestedField.required(
                                        5, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                                Types.NestedField.required(
                                        6, OFFSET_COLUMN_NAME, Types.LongType.get()),
                                Types.NestedField.required(
                                        7, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone())));

        // Verify iceberg table schema
        assertThat(createdTable.schema().toString()).isEqualTo(expectIcebergSchema.toString());

        // Verify partition field and transform
        assertThat(createdTable.spec().fields()).hasSize(2);
        PartitionField firstPartitionField = createdTable.spec().fields().get(0);
        assertThat(firstPartitionField.name()).isEqualTo("order_type");
        assertThat(firstPartitionField.transform().toString()).isEqualTo("identity");

        PartitionField secondPartitionField = createdTable.spec().fields().get(1);
        assertThat(secondPartitionField.name()).isEqualTo(BUCKET_COLUMN_NAME);
        assertThat(secondPartitionField.transform().toString()).isEqualTo("identity");

        // Verify sort field and order
        assertThat(createdTable.sortOrder().fields()).hasSize(1);
        SortField sortField = createdTable.sortOrder().fields().get(0);
        assertThat(sortField.sourceId())
                .isEqualTo(createdTable.schema().findField(OFFSET_COLUMN_NAME).fieldId());
        assertThat(sortField.direction()).isEqualTo(SortDirection.ASC);
    }

    @Test
    void rejectsLogTableWithMultipleBucketKeys() {
        String database = "test_db";
        String tableName = "multi_bucket_log_table";

        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("user_type", DataTypes.STRING())
                        .column("order_type", DataTypes.STRING())
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(3, "user_type", "order_type")
                        .build();

        TablePath tablePath = TablePath.of(database, tableName);

        // Do not allow multiple bucket keys for log table
        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.createTable(
                                        tablePath,
                                        tableDescriptor,
                                        new TestingLakeCatalogContext()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Only one bucket key is supported for Iceberg");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIllegalPartitionKeyType(boolean isPrimaryKeyTable) throws Exception {
        TablePath t1 =
                TablePath.of(
                        "test_db",
                        isPrimaryKeyTable
                                ? "pkIllegalPartitionKeyType"
                                : "logIllegalPartitionKeyType");
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("c0", DataTypes.STRING())
                        .column("c1", DataTypes.BOOLEAN());
        if (isPrimaryKeyTable) {
            builder.primaryKey("c0", "c1");
        }
        List<String> partitionKeys = List.of("c1");
        TableDescriptor.Builder tableDescriptor =
                TableDescriptor.builder()
                        .schema(builder.build())
                        .distributedBy(1, "c0")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        tableDescriptor.partitionedBy(partitionKeys);

        Assertions.assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.createTable(
                                        t1,
                                        tableDescriptor.build(),
                                        new TestingLakeCatalogContext()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessage(
                        "Partition key only support string type for iceberg currently. Column `c1` is not string type.");
    }

    @Test
    void alterTableProperties() {
        String database = "test_alter_table_db";
        String tableName = "test_alter_table";

        Schema flussSchema = Schema.newBuilder().column("id", DataTypes.BIGINT()).build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(3)
                        .property("iceberg.commit.retry.num-retries", "5")
                        .property("table.datalake.freshness", "30s")
                        .build();

        TablePath tablePath = TablePath.of(database, tableName);
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();
        flussIcebergCatalog.createTable(tablePath, tableDescriptor, context);

        Catalog catalog = flussIcebergCatalog.getIcebergCatalog();
        assertThat(catalog.loadTable(TableIdentifier.of(database, tableName)).properties())
                .containsEntry("commit.retry.num-retries", "5")
                .containsEntry("fluss.table.datalake.freshness", "30s")
                .doesNotContainKeys("iceberg.commit.retry.num-retries", "table.datalake.freshness");

        // set new iceberg property
        flussIcebergCatalog.alterTable(
                tablePath,
                List.of(TableChange.set("iceberg.commit.retry.min-wait-ms", "1000")),
                context);
        assertThat(catalog.loadTable(TableIdentifier.of(database, tableName)).properties())
                .containsEntry("commit.retry.min-wait-ms", "1000")
                .containsEntry("commit.retry.num-retries", "5")
                .containsEntry("fluss.table.datalake.freshness", "30s")
                .doesNotContainKeys(
                        "iceberg.commit.retry.min-wait-ms",
                        "iceberg.commit.retry.num-retries",
                        "table.datalake.freshness");

        // update existing properties
        flussIcebergCatalog.alterTable(
                tablePath,
                List.of(
                        TableChange.set("iceberg.commit.retry.num-retries", "10"),
                        TableChange.set("table.datalake.freshness", "23s")),
                context);
        assertThat(catalog.loadTable(TableIdentifier.of(database, tableName)).properties())
                .containsEntry("commit.retry.min-wait-ms", "1000")
                .containsEntry("commit.retry.num-retries", "10")
                .containsEntry("fluss.table.datalake.freshness", "23s")
                .doesNotContainKeys(
                        "iceberg.commit.retry.min-wait-ms",
                        "iceberg.commit.retry.num-retries",
                        "table.datalake.freshness");

        // remove existing properties
        flussIcebergCatalog.alterTable(
                tablePath,
                List.of(
                        TableChange.reset("iceberg.commit.retry.min-wait-ms"),
                        TableChange.reset("table.datalake.freshness")),
                context);
        assertThat(catalog.loadTable(TableIdentifier.of(database, tableName)).properties())
                .containsEntry("commit.retry.num-retries", "10")
                .doesNotContainKeys(
                        "commit.retry.min-wait-ms",
                        "iceberg.commit.retry.min-wait-ms",
                        "table.datalake.freshness",
                        "fluss.table.datalake.freshness");

        // remove non-existing property
        flussIcebergCatalog.alterTable(
                tablePath, List.of(TableChange.reset("iceberg.non-existing.property")), context);
        assertThat(catalog.loadTable(TableIdentifier.of(database, tableName)).properties())
                .containsEntry("commit.retry.num-retries", "10")
                .doesNotContainKeys(
                        "non-existing.property",
                        "iceberg.non-existing.property",
                        "commit.retry.min-wait-ms",
                        "iceberg.commit.retry.min-wait-ms",
                        "table.datalake.freshness",
                        "fluss.table.datalake.freshness");
    }

    @Test
    void alterTablePropertiesWithNonExistingTable() {
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();
        // db & table don't exist
        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.alterTable(
                                        TablePath.of("non_existing_db", "non_existing_table"),
                                        List.of(
                                                TableChange.set(
                                                        "iceberg.commit.retry.min-wait-ms",
                                                        "1000")),
                                        context))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("Table non_existing_db.non_existing_table does not exist.");

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.BIGINT()).build())
                        .distributedBy(3)
                        .property("iceberg.commit.retry.num-retries", "5")
                        .property("table.datalake.freshness", "30s")
                        .build();

        String database = "test_db";
        TablePath tablePath = TablePath.of(database, "test_table");
        flussIcebergCatalog.createTable(tablePath, tableDescriptor, context);

        // database exists but table doesn't exist
        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.alterTable(
                                        TablePath.of(database, "non_existing_table"),
                                        List.of(
                                                TableChange.set(
                                                        "iceberg.commit.retry.min-wait-ms",
                                                        "1000")),
                                        context))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("Table test_db.non_existing_table does not exist.");
    }

    @Test
    void alterReservedTableProperties() {
        String database = "test_alter_table_with_reserved_properties_db";
        String tableName = "test_alter_table_with_reserved_properties";

        Schema flussSchema = Schema.newBuilder().column("id", DataTypes.BIGINT()).build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(3).build();

        TablePath tablePath = TablePath.of(database, tableName);
        TestingLakeCatalogContext context = new TestingLakeCatalogContext();
        flussIcebergCatalog.createTable(tablePath, tableDescriptor, context);

        for (String property : IcebergLakeCatalog.RESERVED_PROPERTIES) {
            assertThatThrownBy(
                            () ->
                                    flussIcebergCatalog.alterTable(
                                            tablePath,
                                            List.of(
                                                    TableChange.set(
                                                            property,
                                                            RowLevelOperationMode.COPY_ON_WRITE
                                                                    .modeName())),
                                            context))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Cannot set table property '%s'", property);

            assertThatThrownBy(
                            () ->
                                    flussIcebergCatalog.alterTable(
                                            tablePath,
                                            List.of(TableChange.reset(property)),
                                            context))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Cannot reset table property '%s'", property);
        }
    }

    @Test
    void testAlterTableAddColumnLastNullable() {
        String database = "test_alter_add_col_db";
        String tableName = "test_alter_add_col_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createLogTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_col",
                                DataTypes.INT(),
                                "new_col comment",
                                TableChange.ColumnPosition.last()));

        flussIcebergCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(FLUSS_SCHEMA, changes));

        Table table =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        List<String> fieldNames =
                table.schema().columns().stream()
                        .map(Types.NestedField::name)
                        .collect(Collectors.toList());
        assertThat(fieldNames)
                .containsExactly(
                        "id",
                        "name",
                        "amount",
                        "address",
                        "new_col",
                        BUCKET_COLUMN_NAME,
                        OFFSET_COLUMN_NAME,
                        TIMESTAMP_COLUMN_NAME);

        // Verify the new column's type and nullability
        Types.NestedField newCol = table.schema().findField("new_col");
        assertThat(newCol.type()).isEqualTo(Types.IntegerType.get());
        assertThat(newCol.isOptional()).isTrue();
        assertThat(newCol.doc()).isEqualTo("new_col comment");
    }

    @Test
    void testAlterTableAddColumnNotLast() {
        String database = "test_alter_add_col_not_last_db";
        String tableName = "test_alter_add_col_not_last_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createLogTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_col",
                                DataTypes.INT(),
                                null,
                                TableChange.ColumnPosition.first()));

        // Use a simple context since SchemaUpdate rejects non-LAST position
        TableDescriptor td = getTableDescriptor(FLUSS_SCHEMA);
        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.alterTable(
                                        tablePath, changes, new TestingLakeCatalogContext(td)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Only support to add column at last for iceberg table.");
    }

    @Test
    void testAlterTableAddColumnNotNullable() {
        String database = "test_alter_add_col_not_nullable_db";
        String tableName = "test_alter_add_col_not_nullable_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createLogTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_col",
                                DataTypes.INT().copy(false),
                                null,
                                TableChange.ColumnPosition.last()));

        // Use a simple context since SchemaUpdate rejects non-nullable columns
        TableDescriptor td = getTableDescriptor(FLUSS_SCHEMA);
        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.alterTable(
                                        tablePath, changes, new TestingLakeCatalogContext(td)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Only support to add nullable column for iceberg table.");
    }

    @Test
    void testAlterTableAddExistingColumns() {
        String database = "test_alter_add_existing_col_db";
        String tableName = "test_alter_add_existing_col_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createLogTable(database, tableName);

        // Simulate crash recovery: add columns to iceberg first, then retry
        List<TableChange> changes =
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

        // First call succeeds - adds columns to iceberg
        flussIcebergCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(FLUSS_SCHEMA, changes));

        // Second call with same changes should succeed (idempotent via schema compatibility)
        // because iceberg schema now matches the expected schema
        flussIcebergCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(FLUSS_SCHEMA, changes));
    }

    @Test
    void testAlterTableAddColumnWhenIcebergSchemaNotMatch() {
        String database = "test_alter_add_col_schema_mismatch_db";
        String tableName = "test_alter_add_col_schema_mismatch_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createLogTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_col",
                                DataTypes.INT(),
                                "new_col comment",
                                TableChange.ColumnPosition.last()));

        // Test column number mismatch: pass a wider Fluss schema that doesn't match iceberg
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
                                flussIcebergCatalog.alterTable(
                                        tablePath,
                                        changes,
                                        getLakeCatalogContext(widerFlussSchema, changes)))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Iceberg schema is not compatible with Fluss schema");

        // Test column order mismatch
        Schema disorderFlussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("amount", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("address", DataTypes.STRING())
                        .build();
        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.alterTable(
                                        tablePath,
                                        changes,
                                        getLakeCatalogContext(disorderFlussSchema, changes)))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Iceberg schema is not compatible with Fluss schema");
    }

    @Test
    void testAlterTableAddColumnWithComplexTypeTable() {
        String database = "test_alter_add_col_complex_db";
        String tableName = "test_alter_add_col_complex_table";
        TablePath tablePath = TablePath.of(database, tableName);

        // Create table with complex types (Map, Array)
        Schema complexSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("tags", DataTypes.ARRAY(DataTypes.STRING()))
                        .column("metadata", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build();

        TableDescriptor td = getTableDescriptor(complexSchema);
        flussIcebergCatalog.createTable(tablePath, td, new TestingLakeCatalogContext());

        // ADD COLUMN should succeed despite Iceberg reassigning field IDs for complex types
        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_col",
                                DataTypes.STRING(),
                                null,
                                TableChange.ColumnPosition.last()));

        flussIcebergCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(complexSchema, changes));

        Table table =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        List<String> fieldNames =
                table.schema().columns().stream()
                        .map(Types.NestedField::name)
                        .collect(Collectors.toList());
        assertThat(fieldNames)
                .containsExactly(
                        "id",
                        "tags",
                        "metadata",
                        "new_col",
                        BUCKET_COLUMN_NAME,
                        OFFSET_COLUMN_NAME,
                        TIMESTAMP_COLUMN_NAME);

        // Verify the new column
        Types.NestedField newCol = table.schema().findField("new_col");
        assertThat(newCol.type()).isEqualTo(Types.StringType.get());
        assertThat(newCol.isOptional()).isTrue();
    }

    @Test
    void testAlterTableAddColumnIdempotencyWithComplexTypes() {
        String database = "test_alter_idempotent_complex_db";
        String tableName = "test_alter_idempotent_complex_table";
        TablePath tablePath = TablePath.of(database, tableName);

        Schema complexSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("data", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .build();

        TableDescriptor td = getTableDescriptor(complexSchema);
        flussIcebergCatalog.createTable(tablePath, td, new TestingLakeCatalogContext());

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_col",
                                DataTypes.INT(),
                                null,
                                TableChange.ColumnPosition.last()));

        // First call succeeds
        flussIcebergCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(complexSchema, changes));

        // Second call (crash recovery) should succeed via idempotency check
        flussIcebergCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(complexSchema, changes));
    }

    @Test
    void testAlterTableAddArrayColumn() {
        String database = "test_alter_add_array_db";
        String tableName = "test_alter_add_array_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createLogTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_arr",
                                DataTypes.ARRAY(DataTypes.STRING()),
                                null,
                                TableChange.ColumnPosition.last()));

        flussIcebergCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(FLUSS_SCHEMA, changes));

        Table table =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        Types.NestedField field = table.schema().findField("new_arr");
        assertThat(field).isNotNull();
        assertThat(field.isOptional()).isTrue();
        assertThat(field.type().isListType()).isTrue();
        Types.ListType listType = field.type().asListType();
        assertThat(listType.elementType()).isEqualTo(Types.StringType.get());
        assertThat(listType.elementId()).isNotEqualTo(field.fieldId());
        assertThat(table.schema().columns())
                .extracting(Types.NestedField::name)
                .containsSubsequence("new_arr", BUCKET_COLUMN_NAME);
    }

    @Test
    void testAlterTableAddMapColumn() {
        String database = "test_alter_add_map_db";
        String tableName = "test_alter_add_map_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createLogTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_map",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                null,
                                TableChange.ColumnPosition.last()));

        flussIcebergCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(FLUSS_SCHEMA, changes));

        Table table =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        Types.NestedField field = table.schema().findField("new_map");
        assertThat(field.type().isMapType()).isTrue();
        Types.MapType mapType = field.type().asMapType();
        assertThat(mapType.keyType()).isEqualTo(Types.StringType.get());
        assertThat(mapType.valueType()).isEqualTo(Types.IntegerType.get());
        assertThat(mapType.keyId()).isNotEqualTo(mapType.valueId());
        assertThat(mapType.keyId()).isNotEqualTo(field.fieldId());
        assertThat(mapType.valueId()).isNotEqualTo(field.fieldId());
    }

    @Test
    void testAlterTableAddRowColumn() {
        String database = "test_alter_add_row_db";
        String tableName = "test_alter_add_row_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createLogTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_row",
                                DataTypes.ROW(
                                        DataTypes.FIELD("a", DataTypes.INT()),
                                        DataTypes.FIELD("b", DataTypes.STRING())),
                                null,
                                TableChange.ColumnPosition.last()));

        flussIcebergCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(FLUSS_SCHEMA, changes));

        Table table =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        Types.NestedField field = table.schema().findField("new_row");
        assertThat(field.type().isStructType()).isTrue();
        Types.StructType struct = field.type().asStructType();
        assertThat(struct.fields()).extracting(Types.NestedField::name).containsExactly("a", "b");
        assertThat(struct.field("a").type()).isEqualTo(Types.IntegerType.get());
        assertThat(struct.field("b").type()).isEqualTo(Types.StringType.get());
        assertThat(struct.field("a").fieldId()).isNotEqualTo(struct.field("b").fieldId());
        assertThat(struct.field("a").fieldId()).isNotEqualTo(field.fieldId());
    }

    @Test
    void testAlterTableAddNestedComplexColumn() {
        String database = "test_alter_add_nested_db";
        String tableName = "test_alter_add_nested_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createLogTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_nested",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("x", DataTypes.INT()),
                                                DataTypes.FIELD("y", DataTypes.STRING()))),
                                null,
                                TableChange.ColumnPosition.last()));

        flussIcebergCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(FLUSS_SCHEMA, changes));

        Table table =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        Types.NestedField field = table.schema().findField("new_nested");
        Types.ListType listType = field.type().asListType();
        Types.StructType struct = listType.elementType().asStructType();
        assertThat(struct.fields()).extracting(Types.NestedField::name).containsExactly("x", "y");

        Set<Integer> ids = new HashSet<>();
        ids.add(field.fieldId());
        ids.add(listType.elementId());
        ids.add(struct.field("x").fieldId());
        ids.add(struct.field("y").fieldId());
        assertThat(ids).hasSize(4);
    }

    @Test
    void testAlterTableAddDecimalColumnPreservesPrecisionScale() {
        String database = "test_alter_add_decimal_db";
        String tableName = "test_alter_add_decimal_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createLogTable(database, tableName);

        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_dec",
                                DataTypes.DECIMAL(20, 5),
                                null,
                                TableChange.ColumnPosition.last()));

        flussIcebergCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(FLUSS_SCHEMA, changes));

        Table table =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        assertThat(table.schema().findField("new_dec").type())
                .isEqualTo(Types.DecimalType.of(20, 5));
    }

    @Test
    void testAlterTableAddTimestampVariants() {
        String database = "test_alter_add_ts_db";
        String tableName = "test_alter_add_ts_table";
        TablePath tablePath = TablePath.of(database, tableName);
        createLogTable(database, tableName);

        List<TableChange> changes =
                Arrays.asList(
                        TableChange.addColumn(
                                "new_ts",
                                DataTypes.TIMESTAMP(6),
                                null,
                                TableChange.ColumnPosition.last()),
                        TableChange.addColumn(
                                "new_ts_ltz",
                                DataTypes.TIMESTAMP_LTZ(6),
                                null,
                                TableChange.ColumnPosition.last()));

        flussIcebergCatalog.alterTable(
                tablePath, changes, getLakeCatalogContext(FLUSS_SCHEMA, changes));

        Table table =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        assertThat(table.schema().findField("new_ts").type())
                .isEqualTo(Types.TimestampType.withoutZone());
        assertThat(table.schema().findField("new_ts_ltz").type())
                .isEqualTo(Types.TimestampType.withZone());
    }

    @Test
    void testAlterTableComplexTypeMismatch() {
        String database = "test_alter_complex_mismatch_db";
        String tableName = "test_alter_complex_mismatch_table";
        TablePath tablePath = TablePath.of(database, tableName);

        Schema actualSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("tags", DataTypes.ARRAY(DataTypes.STRING()))
                        .build();
        flussIcebergCatalog.createTable(
                tablePath, getTableDescriptor(actualSchema), new TestingLakeCatalogContext());

        Schema mismatched =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("tags", DataTypes.ARRAY(DataTypes.INT()))
                        .build();
        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addColumn(
                                "new_col",
                                DataTypes.STRING(),
                                null,
                                TableChange.ColumnPosition.last()));

        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.alterTable(
                                        tablePath,
                                        changes,
                                        getLakeCatalogContext(mismatched, changes)))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Iceberg schema is not compatible with Fluss schema");
    }

    /**
     * If the Iceberg table already exists with a compatible schema, re-creating the same Fluss
     * table should succeed idempotently as long as the caller does not claim it is the first
     * creation of the Fluss table.
     */
    @Test
    void testCreateTableIdempotentWhenCompatibleLakeTableExists() {
        String database = "reuse_existing_db";
        String tableName = "reuse_existing_table";
        TablePath tablePath = TablePath.of(database, tableName);

        TableDescriptor td = getTableDescriptor(FLUSS_SCHEMA);

        // First creation should succeed
        flussIcebergCatalog.createTable(tablePath, td, new TestingLakeCatalogContext());

        // Second creation with the same schema and isCreatingFlussTable=false should succeed
        // idempotently.
        flussIcebergCatalog.createTable(tablePath, td, new TestingLakeCatalogContext());

        Table table =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        assertThat(table).isNotNull();
    }

    /**
     * When creating a fresh Fluss table, if a compatible but empty Iceberg table already exists,
     * the creation should still succeed (allowing a Fluss table to bind to a pre-created empty lake
     * table).
     */
    @Test
    void testCreateFlussTableWithExistingEmptyLakeTable() {
        String database = "empty_lake_db";
        String tableName = "empty_lake_table";
        TablePath tablePath = TablePath.of(database, tableName);

        TableDescriptor td = getTableDescriptor(FLUSS_SCHEMA);

        // First creation: prepares the Iceberg table (still empty, no snapshot)
        flussIcebergCatalog.createTable(tablePath, td, new TestingLakeCatalogContext());
        Table before =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        assertThat(before.currentSnapshot()).isNull();

        // A brand-new Fluss table binding to an existing empty compatible Iceberg table
        // should be allowed.
        TestingLakeCatalogContext firstFlussCreationCtx =
                new TestingLakeCatalogContext() {
                    @Override
                    public boolean isCreatingFlussTable() {
                        return true;
                    }
                };

        flussIcebergCatalog.createTable(tablePath, td, firstFlussCreationCtx);
    }

    /**
     * When creating a fresh Fluss table, if a compatible but non-empty Iceberg table already
     * exists, the creation should fail with a clear "not empty" message.
     */
    @Test
    void testCreateFlussTableFailsWhenExistingLakeTableIsNonEmpty() {
        String database = "non_empty_lake_db";
        String tableName = "non_empty_lake_table";
        TablePath tablePath = TablePath.of(database, tableName);

        TableDescriptor td = getTableDescriptor(FLUSS_SCHEMA);

        // First: create the Iceberg table.
        flussIcebergCatalog.createTable(tablePath, td, new TestingLakeCatalogContext());

        // Then: append a dummy data file so that the table has a snapshot.
        Table icebergTable =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        appendDummyDataFile(icebergTable);
        Table afterAppend =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        assertThat(afterAppend.currentSnapshot()).isNotNull();

        // Now creating a brand-new Fluss table on top of a non-empty Iceberg table must fail.
        TestingLakeCatalogContext firstFlussCreationCtx =
                new TestingLakeCatalogContext() {
                    @Override
                    public boolean isCreatingFlussTable() {
                        return true;
                    }
                };

        assertThatThrownBy(
                        () -> flussIcebergCatalog.createTable(tablePath, td, firstFlussCreationCtx))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("already exists in Iceberg catalog")
                .hasMessageContaining("not empty");
    }

    /**
     * If the existing Iceberg table has an incompatible schema, the creation should fail regardless
     * of whether it is a first Fluss creation or a replay.
     */
    @Test
    void testCreateTableFailsWithIncompatibleLakeSchema() {
        String database = "incompat_lake_db";
        String tableName = "incompat_lake_table";
        TablePath tablePath = TablePath.of(database, tableName);

        // First: create the Iceberg table with the base FLUSS_SCHEMA.
        TableDescriptor tdOrig = getTableDescriptor(FLUSS_SCHEMA);
        flussIcebergCatalog.createTable(tablePath, tdOrig, new TestingLakeCatalogContext());

        // Then: attempt to create a Fluss table pointing at the same lake table, but with a
        // different schema (an extra column). Should be rejected as incompatible.
        Schema mismatchedSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("address", DataTypes.STRING())
                        .column("extra", DataTypes.STRING())
                        .build();
        TableDescriptor tdMismatch = getTableDescriptor(mismatchedSchema);

        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.createTable(
                                        tablePath, tdMismatch, new TestingLakeCatalogContext()))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("already exists in Iceberg catalog")
                .hasMessageContaining("not compatible");
    }

    @Test
    void testIsIcebergSchemaCompatibleWithSchema() {
        // baseline
        org.apache.iceberg.Schema base =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "id", Types.LongType.get()),
                        Types.NestedField.optional(2, "name", Types.StringType.get()));

        // identical schema -> compatible
        org.apache.iceberg.Schema sameStruct =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(11, "id", Types.LongType.get()),
                        Types.NestedField.optional(22, "name", Types.StringType.get()));
        assertThat(flussIcebergCatalog.isIcebergSchemaCompatibleWithSchema(base, sameStruct))
                .isTrue();

        // extra column -> incompatible
        org.apache.iceberg.Schema extra =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "id", Types.LongType.get()),
                        Types.NestedField.optional(2, "name", Types.StringType.get()),
                        Types.NestedField.optional(3, "extra", Types.StringType.get()));
        assertThat(flussIcebergCatalog.isIcebergSchemaCompatibleWithSchema(base, extra)).isFalse();

        // different name -> incompatible
        org.apache.iceberg.Schema renamed =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "id", Types.LongType.get()),
                        Types.NestedField.optional(2, "full_name", Types.StringType.get()));
        assertThat(flussIcebergCatalog.isIcebergSchemaCompatibleWithSchema(base, renamed))
                .isFalse();

        // different type -> incompatible
        org.apache.iceberg.Schema differentType =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "id", Types.LongType.get()),
                        Types.NestedField.optional(2, "name", Types.IntegerType.get()));
        assertThat(flussIcebergCatalog.isIcebergSchemaCompatibleWithSchema(base, differentType))
                .isFalse();

        // different nullability -> incompatible
        org.apache.iceberg.Schema differentNullability =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "id", Types.LongType.get()),
                        Types.NestedField.required(2, "name", Types.StringType.get()));
        assertThat(
                        flussIcebergCatalog.isIcebergSchemaCompatibleWithSchema(
                                base, differentNullability))
                .isFalse();

        org.apache.iceberg.Schema withIdAsIdentifier =
                new org.apache.iceberg.Schema(
                        Arrays.asList(
                                Types.NestedField.required(1, "id", Types.LongType.get()),
                                Types.NestedField.optional(2, "name", Types.StringType.get())),
                        Collections.singleton(1));
        org.apache.iceberg.Schema withIdAsIdentifierDifferentIds =
                new org.apache.iceberg.Schema(
                        Arrays.asList(
                                Types.NestedField.required(11, "id", Types.LongType.get()),
                                Types.NestedField.optional(22, "name", Types.StringType.get())),
                        Collections.singleton(11));
        assertThat(
                        flussIcebergCatalog.isIcebergSchemaCompatibleWithSchema(
                                withIdAsIdentifier, withIdAsIdentifierDifferentIds))
                .isTrue();

        assertThat(
                        flussIcebergCatalog.isIcebergSchemaCompatibleWithSchema(
                                base, withIdAsIdentifier))
                .isFalse();
        assertThat(
                        flussIcebergCatalog.isIcebergSchemaCompatibleWithSchema(
                                withIdAsIdentifier, base))
                .isFalse();

        org.apache.iceberg.Schema withNameAsIdentifier =
                new org.apache.iceberg.Schema(
                        Arrays.asList(
                                Types.NestedField.required(1, "id", Types.LongType.get()),
                                Types.NestedField.required(2, "name", Types.StringType.get())),
                        Collections.singleton(2));
        org.apache.iceberg.Schema withIdAsIdentifierRequiredName =
                new org.apache.iceberg.Schema(
                        Arrays.asList(
                                Types.NestedField.required(1, "id", Types.LongType.get()),
                                Types.NestedField.required(2, "name", Types.StringType.get())),
                        Collections.singleton(1));
        assertThat(
                        flussIcebergCatalog.isIcebergSchemaCompatibleWithSchema(
                                withNameAsIdentifier, withIdAsIdentifierRequiredName))
                .isFalse();

        Set<Integer> twoIdentifiers = new HashSet<>();
        twoIdentifiers.add(1);
        twoIdentifiers.add(2);
        org.apache.iceberg.Schema withTwoIdentifiers =
                new org.apache.iceberg.Schema(
                        Arrays.asList(
                                Types.NestedField.required(1, "id", Types.LongType.get()),
                                Types.NestedField.required(2, "name", Types.StringType.get())),
                        twoIdentifiers);
        assertThat(
                        flussIcebergCatalog.isIcebergSchemaCompatibleWithSchema(
                                withIdAsIdentifierRequiredName, withTwoIdentifiers))
                .isFalse();
    }

    @Test
    void testCreateTableFailsWhenExistingLakeTableMissesIdentifierFields() {
        String database = "missing_identifier_db";
        String tableName = "missing_identifier_table";
        TablePath tablePath = TablePath.of(database, tableName);

        Schema pkSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor pkTd =
                TableDescriptor.builder().schema(pkSchema).distributedBy(4, "id").build();

        // Pre-create the Iceberg table directly (bypassing Fluss) so that identifier fields
        // are NOT set on the schema, even though columns/partition/sort/properties match.
        Catalog rawCatalog = flussIcebergCatalog.getIcebergCatalog();
        org.apache.iceberg.Schema noIdentifierSchema =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "name", Types.StringType.get()),
                        Types.NestedField.required(3, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                        Types.NestedField.required(4, OFFSET_COLUMN_NAME, Types.LongType.get()),
                        Types.NestedField.required(
                                5, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone()));
        PartitionSpec spec = PartitionSpec.builderFor(noIdentifierSchema).bucket("id", 4).build();
        SortOrder sortOrder =
                SortOrder.builderFor(noIdentifierSchema).asc(OFFSET_COLUMN_NAME).build();
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.MERGE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        props.put(TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        props.put(TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        ((SupportsNamespaces) rawCatalog).createNamespace(Namespace.of(database));
        rawCatalog
                .buildTable(TableIdentifier.of(database, tableName), noIdentifierSchema)
                .withPartitionSpec(spec)
                .withSortOrder(sortOrder)
                .withProperties(props)
                .create();

        // Now Fluss tries to bind a PK table on top. The identifier-field mismatch must be
        // surfaced as a schema incompatibility.
        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.createTable(
                                        tablePath, pkTd, new TestingLakeCatalogContext()))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("already exists in Iceberg catalog")
                .hasMessageContaining("not compatible");
    }

    @Test
    void testCreateTableFailsWhenExistingLakeTableHasWrongIdentifierFields() {
        String database = "wrong_identifier_db";
        String tableName = "wrong_identifier_table";
        TablePath tablePath = TablePath.of(database, tableName);

        Schema pkSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING().copy(false))
                        .primaryKey("id")
                        .build();
        TableDescriptor pkTd =
                TableDescriptor.builder().schema(pkSchema).distributedBy(4, "id").build();

        // Pre-create the Iceberg table with identifier fields pointing to "name" instead
        // of the Fluss primary key "id".
        Catalog rawCatalog = flussIcebergCatalog.getIcebergCatalog();
        org.apache.iceberg.Schema wrongIdentifierSchema =
                new org.apache.iceberg.Schema(
                        Arrays.asList(
                                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                                Types.NestedField.required(2, "name", Types.StringType.get()),
                                Types.NestedField.required(
                                        3, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                                Types.NestedField.required(
                                        4, OFFSET_COLUMN_NAME, Types.LongType.get()),
                                Types.NestedField.required(
                                        5, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone())),
                        Collections.singleton(2));
        PartitionSpec spec =
                PartitionSpec.builderFor(wrongIdentifierSchema).bucket("id", 4).build();
        SortOrder sortOrder =
                SortOrder.builderFor(wrongIdentifierSchema).asc(OFFSET_COLUMN_NAME).build();
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.MERGE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        props.put(TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        props.put(TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        ((SupportsNamespaces) rawCatalog).createNamespace(Namespace.of(database));
        rawCatalog
                .buildTable(TableIdentifier.of(database, tableName), wrongIdentifierSchema)
                .withPartitionSpec(spec)
                .withSortOrder(sortOrder)
                .withProperties(props)
                .create();

        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.createTable(
                                        tablePath, pkTd, new TestingLakeCatalogContext()))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("already exists in Iceberg catalog")
                .hasMessageContaining("not compatible");
    }

    /**
     * Simulates the real-world case where a user pre-creates the Iceberg table with a different
     * bucket count (partition transform {@code bucket[N]}) than what Fluss expects, then attempts
     * to bind a Fluss table on top. The mismatch on the partition transform must be detected and
     * rejected.
     */
    @Test
    void testCreateTableFailsWithIncompatiblePartitionBucketCount() {
        String database = "spec_bucket_db";
        String tableName = "spec_bucket_table";
        TablePath tablePath = TablePath.of(database, tableName);

        // Pre-create the Iceberg table directly with bucket count = 2.
        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor td2Buckets =
                TableDescriptor.builder().schema(flussSchema).distributedBy(2, "id").build();
        flussIcebergCatalog.createTable(tablePath, td2Buckets, new TestingLakeCatalogContext());

        // Now Fluss tries to (re-)create the same table with bucket count = 4.
        TableDescriptor td4Buckets =
                TableDescriptor.builder().schema(flussSchema).distributedBy(4, "id").build();

        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.createTable(
                                        tablePath, td4Buckets, new TestingLakeCatalogContext()))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("already exists in Iceberg catalog")
                .hasMessageContaining("partition spec is not compatible");
    }

    /**
     * When a log table has partition keys, mismatched partition columns should be rejected as
     * partition spec incompatible.
     */
    @Test
    void testCreateTableFailsWithIncompatiblePartitionKeys() {
        String database = "spec_partkeys_db";
        String tableName = "spec_partkeys_table";
        TablePath tablePath = TablePath.of(database, tableName);

        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .column("region", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .build();

        // First: partitioned by "region"
        TableDescriptor tdRegion =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(3)
                        .partitionedBy("region")
                        .build();
        flussIcebergCatalog.createTable(tablePath, tdRegion, new TestingLakeCatalogContext());

        // Second: partitioned by "dt" -> mismatched
        TableDescriptor tdDt =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(3)
                        .partitionedBy("dt")
                        .build();
        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.createTable(
                                        tablePath, tdDt, new TestingLakeCatalogContext()))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("partition spec is not compatible");
    }

    /**
     * If someone alters the sort order of the existing Iceberg table (e.g. removes the fluss sort
     * order), Fluss should detect the mismatch when re-binding.
     */
    @Test
    void testCreateTableFailsWithIncompatibleSortOrder() {
        String database = "sortorder_db";
        String tableName = "sortorder_table";
        TablePath tablePath = TablePath.of(database, tableName);

        TableDescriptor td = getTableDescriptor(FLUSS_SCHEMA);
        flussIcebergCatalog.createTable(tablePath, td, new TestingLakeCatalogContext());

        // Manually replace the sort order of the existing Iceberg table.
        Table iceberg =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        iceberg.replaceSortOrder().asc("id").commit();

        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.createTable(
                                        tablePath, td, new TestingLakeCatalogContext()))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("sort order is not compatible")
                .hasMessageContaining("ASC(__offset)")
                .hasMessageContaining("pre-create the Iceberg table without a custom sort order");
    }

    /**
     * If someone changes a fluss-managed property on the existing Iceberg table (e.g. write mode),
     * the mismatch should be surfaced.
     */
    @Test
    void testCreateTableFailsWithIncompatibleProperties() {
        String database = "props_db";
        String tableName = "props_table";
        TablePath tablePath = TablePath.of(database, tableName);

        // pk table -> we set MERGE_MODE / UPDATE_MODE / DELETE_MODE to merge-on-read.
        Schema pkSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor td =
                TableDescriptor.builder().schema(pkSchema).distributedBy(4, "id").build();
        flussIcebergCatalog.createTable(tablePath, td, new TestingLakeCatalogContext());

        // Manually change a required property to an incompatible value.
        Table iceberg =
                flussIcebergCatalog
                        .getIcebergCatalog()
                        .loadTable(TableIdentifier.of(database, tableName));
        iceberg.updateProperties()
                .set(TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName())
                .commit();

        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.createTable(
                                        tablePath, td, new TestingLakeCatalogContext()))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("table properties are not compatible");
    }

    /** White-box tests for {@link IcebergLakeCatalog#isIcebergPartitionSpecCompatible}. */
    @Test
    void testIsIcebergPartitionSpecCompatible() {
        // baseline schema with an "id" column (required).
        org.apache.iceberg.Schema schemaA =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "id", Types.LongType.get()),
                        Types.NestedField.optional(2, "name", Types.StringType.get()));
        org.apache.iceberg.Schema schemaB =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(11, "id", Types.LongType.get()),
                        Types.NestedField.optional(22, "name", Types.StringType.get()));

        PartitionSpec bucket4A = PartitionSpec.builderFor(schemaA).bucket("id", 4).build();
        PartitionSpec bucket4B = PartitionSpec.builderFor(schemaB).bucket("id", 4).build();
        PartitionSpec bucket2A = PartitionSpec.builderFor(schemaA).bucket("id", 2).build();
        PartitionSpec identityNameA = PartitionSpec.builderFor(schemaA).identity("name").build();

        // Same transform + same source column, different field ids -> compatible.
        assertThat(
                        flussIcebergCatalog.isIcebergPartitionSpecCompatible(
                                bucket4A, schemaA, bucket4B, schemaB))
                .isTrue();

        // Different bucket count on same column -> incompatible.
        assertThat(
                        flussIcebergCatalog.isIcebergPartitionSpecCompatible(
                                bucket4A, schemaA, bucket2A, schemaA))
                .isFalse();

        // Different source column -> incompatible.
        assertThat(
                        flussIcebergCatalog.isIcebergPartitionSpecCompatible(
                                bucket4A, schemaA, identityNameA, schemaA))
                .isFalse();

        // Different partition field count -> incompatible.
        PartitionSpec twoFields =
                PartitionSpec.builderFor(schemaA).identity("name").bucket("id", 4).build();
        assertThat(
                        flussIcebergCatalog.isIcebergPartitionSpecCompatible(
                                bucket4A, schemaA, twoFields, schemaA))
                .isFalse();
    }

    /** White-box tests for {@link IcebergLakeCatalog#isIcebergSortOrderCompatible}. */
    @Test
    void testIsIcebergSortOrderCompatible() {
        org.apache.iceberg.Schema schemaA =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "id", Types.LongType.get()),
                        Types.NestedField.optional(2, "name", Types.StringType.get()));
        org.apache.iceberg.Schema schemaB =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(11, "id", Types.LongType.get()),
                        Types.NestedField.optional(22, "name", Types.StringType.get()));

        SortOrder ascIdA = SortOrder.builderFor(schemaA).asc("id").build();
        SortOrder ascIdB = SortOrder.builderFor(schemaB).asc("id").build();
        SortOrder descIdA = SortOrder.builderFor(schemaA).desc("id").build();
        SortOrder ascNameA = SortOrder.builderFor(schemaA).asc("name").build();

        // Same source + direction, different ids -> compatible.
        assertThat(
                        flussIcebergCatalog.isIcebergSortOrderCompatible(
                                ascIdA, schemaA, ascIdB, schemaB))
                .isTrue();

        // Different direction -> incompatible.
        assertThat(
                        flussIcebergCatalog.isIcebergSortOrderCompatible(
                                ascIdA, schemaA, descIdA, schemaA))
                .isFalse();

        // Different source column -> incompatible.
        assertThat(
                        flussIcebergCatalog.isIcebergSortOrderCompatible(
                                ascIdA, schemaA, ascNameA, schemaA))
                .isFalse();

        // Different sort field count -> incompatible.
        SortOrder twoFields = SortOrder.builderFor(schemaA).asc("id").asc("name").build();
        assertThat(
                        flussIcebergCatalog.isIcebergSortOrderCompatible(
                                ascIdA, schemaA, twoFields, schemaA))
                .isFalse();
    }

    /** White-box tests for {@link IcebergLakeCatalog#isIcebergPropertiesCompatible}. */
    @Test
    void testIsIcebergPropertiesCompatible() {
        Map<String, String> expected = new HashMap<>();
        expected.put("k1", "v1");
        expected.put("k2", "v2");

        // Existing has all expected keys with same values -> compatible.
        Map<String, String> existing = new HashMap<>(expected);
        assertThat(flussIcebergCatalog.isIcebergPropertiesCompatible(existing, expected)).isTrue();

        // Existing also has extra keys -> still compatible (extras are ignored).
        existing = new HashMap<>(expected);
        existing.put("engine.internal", "whatever");
        assertThat(flussIcebergCatalog.isIcebergPropertiesCompatible(existing, expected)).isTrue();

        // Existing missing an expected key -> incompatible.
        existing = new HashMap<>();
        existing.put("k1", "v1");
        assertThat(flussIcebergCatalog.isIcebergPropertiesCompatible(existing, expected)).isFalse();

        // Existing has key but different value -> incompatible.
        existing = new HashMap<>(expected);
        existing.put("k2", "different");
        assertThat(flussIcebergCatalog.isIcebergPropertiesCompatible(existing, expected)).isFalse();
    }

    private void appendDummyDataFile(Table icebergTable) {
        DataFile dataFile =
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/tmp/fluss-fake-file.parquet")
                        .withFileSizeInBytes(1024)
                        .withRecordCount(1L)
                        .withFormat(FileFormat.PARQUET)
                        .build();
        icebergTable.newAppend().appendFile(dataFile).commit();
    }

    private void createLogTable(String database, String tableName) {
        TableDescriptor td = getTableDescriptor(FLUSS_SCHEMA);
        TablePath tablePath = TablePath.of(database, tableName);
        flussIcebergCatalog.createTable(tablePath, td, new TestingLakeCatalogContext());
    }

    private TestingLakeCatalogContext getLakeCatalogContext(
            Schema schema, List<TableChange> schemaChanges) {
        Schema expectedSchema = SchemaUpdate.applySchemaChanges(schema, schemaChanges);
        return new TestingLakeCatalogContext(
                getTableDescriptor(schema), getTableDescriptor(expectedSchema));
    }

    private TableDescriptor getTableDescriptor(Schema schema) {
        return TableDescriptor.builder().schema(schema).distributedBy(3).build();
    }
}
