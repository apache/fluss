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

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Unit test for {@link IcebergLakeCatalog}. */
class IcebergLakeCatalogTest {

    @TempDir private File tempWarehouseDir;

    private Configuration configuration;
    private IcebergLakeCatalog flussIcebergCatalog;

    @BeforeEach
    void setupCatalog() {
        configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toURI().toString());
        configuration.setString("type", "org.apache.iceberg.inmemory.InMemoryCatalog");
        configuration.setString("name", "fluss_test_catalog");

        this.flussIcebergCatalog = new IcebergLakeCatalog(configuration);
    }

    @Test
    void verifiesBasicTableIdentifierAndWarehouseConfig() {
        TableIdentifier tableId = TableIdentifier.of("test_db", "simple_table");

        assertThat(tableId.name()).isEqualTo("simple_table");
        assertThat(tableId.namespace().toString()).isEqualTo("test_db");

        assertThat(configuration.toMap().get("warehouse"))
                .isEqualTo(tempWarehouseDir.toURI().toString());
        assertThat(flussIcebergCatalog).isNotNull();
    }

    @Test
    void createsTableInNewNamespaceSuccessfully() throws Exception {
        String database = "test_db";
        String tableName = "simple_table";

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
        Table createdTable = flussIcebergCatalog.getIcebergCatalog().loadTable(tableId);

        assertThat(createdTable).isNotNull();
        assertThat(createdTable.name()).endsWith(tableName);

        // 2 business + 3 system = 5
        assertThat(createdTable.schema().columns()).hasSize(5);

        assertThat(createdTable.schema().findField("id")).isNotNull();
        assertThat(createdTable.schema().findField("name")).isNotNull();

        assertThat(createdTable.schema().findField(BUCKET_COLUMN_NAME)).isNotNull();
        assertThat(createdTable.schema().findField(OFFSET_COLUMN_NAME)).isNotNull();
        assertThat(createdTable.schema().findField(TIMESTAMP_COLUMN_NAME)).isNotNull();
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
    void createsPrimaryKeyTableBucketPartitionedOnly() throws Exception {
        String database = "test_db";
        String tableName = "pk_table";

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

        TablePath tablePath = TablePath.of(database, tableName);

        flussIcebergCatalog.createTable(tablePath, tableDescriptor);

        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        Table createdTable = flussIcebergCatalog.getIcebergCatalog().loadTable(tableId);

        assertThat(createdTable.schema().columns()).hasSize(7); // 4 business + 3 system

        assertThat(createdTable.schema().findField("shop_id")).isNotNull();
        assertThat(createdTable.schema().findField("user_id")).isNotNull();
        assertThat(createdTable.schema().findField("num_orders")).isNotNull();
        assertThat(createdTable.schema().findField("total_amount")).isNotNull();

        assertThat(createdTable.schema().findField(BUCKET_COLUMN_NAME)).isNotNull();
        assertThat(createdTable.schema().findField(OFFSET_COLUMN_NAME)).isNotNull();
        assertThat(createdTable.schema().findField(TIMESTAMP_COLUMN_NAME)).isNotNull();

        // Verify partition spec
        assertThat(createdTable.spec().fields()).hasSize(1);
        assertThat(createdTable.spec().fields().get(0).name()).isEqualTo("user_id_bucket");
        assertThat(createdTable.spec().fields().get(0).transform().toString())
                .contains("bucket[4]");

        // Verify sort order
        assertThat(createdTable.sortOrder().fields()).hasSize(1);
        assertThat(createdTable.sortOrder().fields().get(0).sourceId())
                .isEqualTo(createdTable.schema().findField(OFFSET_COLUMN_NAME).fieldId());

        // Verify  table properties
        assertThat(createdTable.properties())
                .containsEntry("write.merge.mode", "merge-on-read")
                .containsEntry("write.delete.mode", "merge-on-read")
                .containsEntry("write.update.mode", "merge-on-read");
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
                        () -> {
                            flussIcebergCatalog.createTable(tablePath, tableDescriptor);
                        })
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to create Iceberg table")
                .hasCauseInstanceOf(UnsupportedOperationException.class)
                .getCause()
                .hasMessageContaining("Only one bucket key is supported for Iceberg");
    }
}
