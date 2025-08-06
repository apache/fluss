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

package com.apache.fluss.lake.iceberg.catalog;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.iceberg.IcebergLakeCatalog;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataTypes;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Unit test for {@link IcebergLakeCatalog}. */
class IcebergLakeCatalogTest {

    @TempDir private File tempWarehouseDir;

    private Configuration configuration;
    private Catalog icebergCatalog;
    private SupportsNamespaces namespaceSupport;
    private IcebergLakeCatalog flussIcebergCatalog;

    @BeforeEach
    void setupCatalog() {
        configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toURI().toString());

        InMemoryCatalog inMemoryCatalog = new InMemoryCatalog();
        inMemoryCatalog.initialize(
                "test_catalog", ImmutableMap.of("warehouse", tempWarehouseDir.toURI().toString()));

        this.icebergCatalog = inMemoryCatalog;
        this.namespaceSupport = inMemoryCatalog;
        this.flussIcebergCatalog = new IcebergLakeCatalog(inMemoryCatalog);
    }

    @Test
    void verifiesBasicTableIdentifierAndWarehouseConfig() {
        TableIdentifier tableId = TableIdentifier.of("test_db", "simple_table");

        assertThat(tableId.name()).isEqualTo("simple_table");
        assertThat(tableId.namespace().toString()).isEqualTo("test_db");

        assertThat(configuration.toMap().get("warehouse"))
                .isEqualTo(tempWarehouseDir.toURI().toString());
        assertThat(icebergCatalog).isNotNull();
    }

    @Test
    void createsTableInNewNamespaceSuccessfully() {
        String database = "test_db";
        String tableName = "simple_table";

        createNamespaceIfAbsent(database);

        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        org.apache.iceberg.Schema schema =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "name", Types.StringType.get()));

        Table createdTable = icebergCatalog.createTable(tableId, schema);

        assertThat(createdTable).isNotNull();
        assertThat(createdTable.name()).endsWith(tableName);
        assertThat(createdTable.schema().columns())
                .hasSize(2)
                .extracting("name")
                .containsExactly("id", "name");
    }

    @Test
    void convertsFlussSchemaToIcebergWithSystemColumns() {
        Schema flussSchema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT())
                        .column("customer_name", DataTypes.STRING())
                        .column("created_at", DataTypes.TIMESTAMP_LTZ()) // timestamp with zone
                        .column("processed_at", DataTypes.TIMESTAMP()) // timestamp without zone
                        .primaryKey("order_id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(4, "order_id").build();

        org.apache.iceberg.Schema icebergSchema =
                flussIcebergCatalog.convertToIcebergSchema(tableDescriptor);

        assertThat(icebergSchema.findField("order_id")).isNotNull();
        assertThat(icebergSchema.findField("customer_name")).isNotNull();
        assertThat(icebergSchema.findField("created_at")).isNotNull();
        assertThat(icebergSchema.findField("processed_at")).isNotNull();

        assertThat(icebergSchema.findField("created_at").type())
                .isEqualTo(Types.TimestampType.withZone());
        assertThat(icebergSchema.findField("processed_at").type())
                .isEqualTo(Types.TimestampType.withoutZone());

        assertThat(icebergSchema.findField(BUCKET_COLUMN_NAME)).isNotNull();
        assertThat(icebergSchema.findField(OFFSET_COLUMN_NAME)).isNotNull();
        assertThat(icebergSchema.findField(TIMESTAMP_COLUMN_NAME)).isNotNull();

        assertThat(icebergSchema.columns()).hasSize(7);
    }

    @Test
    void createsPrimaryKeyTableBucketPartitionedOnly() {
        String database = "test_db";
        String tableName = "pk_table";

        createNamespaceIfAbsent(database);

        // primary key table
        Schema flussSchema =
                Schema.newBuilder()
                        .column("shop_id", DataTypes.BIGINT())
                        .column("user_id", DataTypes.BIGINT())
                        .column("num_orders", DataTypes.INT())
                        .column("total_amount", DataTypes.INT())
                        .primaryKey("user_id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(4, "user_id").build();

        TableIdentifier tableId = TableIdentifier.of(database, tableName);

        org.apache.iceberg.Schema icebergSchema =
                flussIcebergCatalog.convertToIcebergSchema(tableDescriptor);

        PartitionSpec expectedPartitionSpec =
                flussIcebergCatalog.createPartitionSpec(tableDescriptor, icebergSchema);

        SortOrder expectedSortOrder =
                SortOrder.builderFor(icebergSchema).asc(OFFSET_COLUMN_NAME).build();

        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("write.merge.mode", "merge-on-read");

        Table createdTable =
                icebergCatalog
                        .buildTable(tableId, icebergSchema)
                        .withPartitionSpec(expectedPartitionSpec)
                        .withSortOrder(expectedSortOrder)
                        .withProperties(tableProperties)
                        .create();

        assertThat(createdTable.spec().fields()).hasSize(1);
        assertThat(createdTable.spec().fields().get(0).name()).isEqualTo("user_id_bucket");
        assertThat(createdTable.spec().fields().get(0).transform().toString())
                .contains("bucket[4]");

        assertThat(createdTable.sortOrder().fields()).hasSize(1);
        assertThat(createdTable.sortOrder().fields().get(0).sourceId())
                .isEqualTo(icebergSchema.findField(OFFSET_COLUMN_NAME).fieldId());

        assertThat(createdTable.properties()).containsEntry("write.merge.mode", "merge-on-read");
    }

    @Test
    void rejectsPrimaryKeyTableWithMultipleBucketKeys() {
        String database = "test_db";
        String tableName = "multi_bucket_pk_table";

        createNamespaceIfAbsent(database);

        Schema flussSchema =
                Schema.newBuilder()
                        .column("user_id", DataTypes.BIGINT())
                        .column("shop_id", DataTypes.BIGINT())
                        .column("order_id", DataTypes.BIGINT())
                        .column("amount", DataTypes.DOUBLE())
                        .primaryKey("user_id", "shop_id") // Composite primary key
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(4, "user_id", "shop_id") // Multiple bucket keys
                        .build();

        TableIdentifier tableId = TableIdentifier.of(database, tableName);

        assertThatThrownBy(
                        () -> {
                            org.apache.iceberg.Schema icebergSchema =
                                    flussIcebergCatalog.convertToIcebergSchema(tableDescriptor);
                            PartitionSpec partitionSpec =
                                    flussIcebergCatalog.createPartitionSpec(
                                            tableDescriptor, icebergSchema);
                            icebergCatalog.createTable(tableId, icebergSchema, partitionSpec);
                        })
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Only one bucket key is supported for Iceberg");
    }

    @Test
    void verifiesFlussCreatesValidIcebergTable() throws Exception {
        String database = "test_db";

        createNamespaceIfAbsent(database);

        Schema flussSchema =
                Schema.newBuilder()
                        .column("user_id", DataTypes.BIGINT())
                        .column("amount", DataTypes.DOUBLE())
                        .primaryKey("user_id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(4, "user_id").build();

        // Create via Fluss iceberg catalog
        TablePath flussTablePath = TablePath.of(database, "fluss_created");
        flussIcebergCatalog.createTable(flussTablePath, tableDescriptor);

        // Create equivalent via raw Iceberg catalog
        org.apache.iceberg.Schema pureIcebergSchema =
                new org.apache.iceberg.Schema(
                        Types.NestedField.optional(1, "user_id", Types.LongType.get()),
                        Types.NestedField.optional(2, "amount", Types.DoubleType.get()));

        TableIdentifier pureTableId = TableIdentifier.of(database, "pure_iceberg");
        Table pureIcebergTable = icebergCatalog.createTable(pureTableId, pureIcebergSchema);

        TableIdentifier flussTableId = TableIdentifier.of(database, "fluss_created");
        Table flussCreatedTable = icebergCatalog.loadTable(flussTableId);

        assertThat(pureIcebergTable).isNotNull();
        assertThat(flussCreatedTable).isNotNull();

        // Verify Fluss table has additional features pure Iceberg doesn't
        assertThat(pureIcebergTable.schema().columns()).hasSize(2); // Just business columns
        assertThat(flussCreatedTable.schema().columns()).hasSize(5); // Business + system columns

        // both readable by standard Iceberg APIs
        assertThat(pureIcebergTable.location()).isNotNull();
        assertThat(flussCreatedTable.location()).isNotNull();
        assertThat(pureIcebergTable.currentSnapshot()).isNull();
        assertThat(flussCreatedTable.currentSnapshot()).isNull();
    }

    @Test
    void createsFlussTableWithIcebergIntegration() throws Exception {
        String database = "test_db";
        String tableName = "fluss_integration_table";

        createNamespaceIfAbsent(database);

        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(4, "id").build();

        TablePath tablePath = TablePath.of(database, tableName);

        flussIcebergCatalog.createTable(tablePath, tableDescriptor);

        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        Table createdTable = icebergCatalog.loadTable(tableId);

        assertThat(createdTable).isNotNull();
        assertThat(createdTable.name()).endsWith(tableName);

        assertThat(createdTable.schema().columns()).hasSize(5);

        // Verify business columns
        assertThat(createdTable.schema().findField("id")).isNotNull();
        assertThat(createdTable.schema().findField("name")).isNotNull();

        // system columns were added by Fluss
        assertThat(createdTable.schema().findField(BUCKET_COLUMN_NAME)).isNotNull();
        assertThat(createdTable.schema().findField(OFFSET_COLUMN_NAME)).isNotNull();
        assertThat(createdTable.schema().findField(TIMESTAMP_COLUMN_NAME)).isNotNull();

        // Fluss-specific features
        assertThat(createdTable.spec().fields()).hasSize(1); // Bucket partitioning
        assertThat(createdTable.sortOrder().fields()).hasSize(1); // Sort by __offset
        assertThat(createdTable.properties()).containsEntry("write.merge.mode", "merge-on-read");
    }

    private void createNamespaceIfAbsent(String databaseName) {
        Namespace namespace = Namespace.of(databaseName);

        if (!namespaceSupport.namespaceExists(namespace)) {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("location", tempWarehouseDir.toURI() + "/" + databaseName);
            namespaceSupport.createNamespace(namespace, metadata);
        }
    }
}
