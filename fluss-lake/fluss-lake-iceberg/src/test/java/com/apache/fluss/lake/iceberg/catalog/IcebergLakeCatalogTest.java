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
import org.apache.iceberg.types.Type;
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

/** Unit test for {@link IcebergLakeCatalog}. */
class IcebergLakeCatalogTest {

    @TempDir private File tempWarehouseDir;

    private Configuration configuration;
    private Catalog icebergCatalog;
    private SupportsNamespaces namespaceSupport;

    @BeforeEach
    void setupCatalog() {
        configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toURI().toString());

        InMemoryCatalog inMemoryCatalog = new InMemoryCatalog();
        inMemoryCatalog.initialize(
                "test_catalog", ImmutableMap.of("warehouse", tempWarehouseDir.toURI().toString()));

        this.icebergCatalog = inMemoryCatalog;
        this.namespaceSupport = inMemoryCatalog;
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

        org.apache.iceberg.Schema icebergSchema = convertFlussToIcebergSchema(tableDescriptor);

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
        org.apache.iceberg.Schema icebergSchema = convertFlussToIcebergSchema(tableDescriptor);

        PartitionSpec expectedPartitionSpec =
                PartitionSpec.builderFor(icebergSchema).bucket("user_id", 4).build();

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

    private void createNamespaceIfAbsent(String databaseName) {
        Namespace namespace = Namespace.of(databaseName);

        if (!namespaceSupport.namespaceExists(namespace)) {
            Map<String, String> metadata =
                    Map.of("location", tempWarehouseDir.toURI() + "/" + databaseName);
            namespaceSupport.createNamespace(namespace, metadata);
        }
    }

    private org.apache.iceberg.Schema convertFlussToIcebergSchema(TableDescriptor tableDescriptor) {
        int fieldId = 1;

        Types.NestedField[] businessFields =
                new Types.NestedField[tableDescriptor.getSchema().getColumns().size()];
        int i = 0;
        for (Schema.Column column : tableDescriptor.getSchema().getColumns()) {
            businessFields[i] =
                    Types.NestedField.optional(
                            fieldId++,
                            column.getName(),
                            convertFlussTypeToIcebergType(column.getDataType()));
            i++;
        }

        Types.NestedField bucketField =
                Types.NestedField.required(fieldId++, BUCKET_COLUMN_NAME, Types.IntegerType.get());
        Types.NestedField offsetField =
                Types.NestedField.required(fieldId++, OFFSET_COLUMN_NAME, Types.LongType.get());
        Types.NestedField timestampField =
                Types.NestedField.required(
                        fieldId, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone());

        Types.NestedField[] allFields = new Types.NestedField[businessFields.length + 3];
        System.arraycopy(businessFields, 0, allFields, 0, businessFields.length);
        allFields[businessFields.length] = bucketField;
        allFields[businessFields.length + 1] = offsetField;
        allFields[businessFields.length + 2] = timestampField;

        return new org.apache.iceberg.Schema(allFields);
    }

    private Type convertFlussTypeToIcebergType(com.alibaba.fluss.types.DataType flussType) {
        switch (flussType.getTypeRoot()) {
            case INTEGER:
                return Types.IntegerType.get();
            case BIGINT:
                return Types.LongType.get();
            case STRING:
                return Types.StringType.get();
            case BOOLEAN:
                return Types.BooleanType.get();
            case DOUBLE:
                return Types.DoubleType.get();
            case FLOAT:
                return Types.FloatType.get();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Types.TimestampType.withZone();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return Types.TimestampType.withoutZone();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Fluss data type: " + flussType.getTypeRoot());
        }
    }
}
