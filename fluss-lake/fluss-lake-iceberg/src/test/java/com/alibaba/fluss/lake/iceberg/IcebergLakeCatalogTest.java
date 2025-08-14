/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.iceberg;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataTypes;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
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
        flussIcebergCatalog.createTable(tablePath, tableDescriptor);

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

        flussIcebergCatalog.createTable(tablePath, tableDescriptor);

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
                        .column("shop_id", DataTypes.BIGINT())
                        .column("user_id", DataTypes.BIGINT())
                        .column("num_orders", DataTypes.INT())
                        .column("total_amount", DataTypes.INT().copy(false))
                        .primaryKey("shop_id", "user_id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(10)
                        .partitionedBy("shop_id")
                        .property("iceberg.write.format.default", "orc")
                        .property("fluss_k1", "fluss_v1")
                        .build();

        TablePath tablePath = TablePath.of(database, tableName);

        flussIcebergCatalog.createTable(tablePath, tableDescriptor);

        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        Table createdTable = flussIcebergCatalog.getIcebergCatalog().loadTable(tableId);

        Set<Integer> identifierFieldIds = new HashSet<>();
        identifierFieldIds.add(1);
        identifierFieldIds.add(2);
        org.apache.iceberg.Schema expectIcebergSchema =
                new org.apache.iceberg.Schema(
                        Arrays.asList(
                                Types.NestedField.required(1, "shop_id", Types.LongType.get()),
                                Types.NestedField.required(2, "user_id", Types.LongType.get()),
                                Types.NestedField.optional(
                                        3, "num_orders", Types.IntegerType.get()),
                                Types.NestedField.required(
                                        4, "total_amount", Types.IntegerType.get()),
                                Types.NestedField.required(
                                        5, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                                Types.NestedField.required(
                                        6, OFFSET_COLUMN_NAME, Types.LongType.get()),
                                Types.NestedField.required(
                                        7, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone())),
                        identifierFieldIds);
        assertThat(createdTable.schema().toString()).isEqualTo(expectIcebergSchema.toString());

        // Verify partition spec
        assertThat(createdTable.spec().fields()).hasSize(2);
        // first should be partitioned by the fluss partition key
        PartitionField partitionField1 = createdTable.spec().fields().get(0);
        assertThat(partitionField1.name()).isEqualTo("shop_id");
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

        assertThatThrownBy(() -> flussIcebergCatalog.createTable(tablePath, tableDescriptor))
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
        flussIcebergCatalog.createTable(tablePath, td);

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
        flussIcebergCatalog.createTable(path, td);

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
        assertThatThrownBy(() -> flussIcebergCatalog.createTable(tablePath, tableDescriptor))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Only one bucket key is supported for Iceberg");
    }
}
