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
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Unit test for {@link IcebergLakeCatalog}. */
class IcebergLakeCatalogTest {

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
}
