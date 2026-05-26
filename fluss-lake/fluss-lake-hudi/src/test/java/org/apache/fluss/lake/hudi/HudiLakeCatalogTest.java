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
import org.apache.fluss.lake.hudi.utils.HudiConversions;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
}
