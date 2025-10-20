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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link PaimonLakeCatalog}. */
class PaimonLakeCatalogTest {

    @TempDir private File tempWarehouseDir;

    private PaimonLakeCatalog flussPaimonCatalog;

    @BeforeEach
    public void setUp() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toURI().toString());
        flussPaimonCatalog = new PaimonLakeCatalog(configuration);
    }

    @Test
    void testGetTable() {
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("k1", "v1");
        customProperties.put("paimon.file.format", "parquet");
        // customProperties.put("paimon.changelog-producer", "NONE");

        // test bucket key log table
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("log_c1", DataTypes.INT())
                                        .column("log_c2", DataTypes.STRING())
                                        .primaryKey("log_c1", "log_c2")
                                        .build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .distributedBy(3, "log_c1", "log_c2")
                        .build();
        TablePath logTablePath = TablePath.of("fluss", "log_table");
        flussPaimonCatalog.createTable(logTablePath, tableDescriptor);

        TableDescriptor paimonDescriptor = flussPaimonCatalog.getTable(logTablePath);

        verifyTableDescriptor(tableDescriptor, paimonDescriptor);
    }

    @Test
    void testGetTableWithAllTypes() {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("log_c1", DataTypes.BOOLEAN())
                                        .column("log_c2", DataTypes.TINYINT())
                                        .column("log_c3", DataTypes.SMALLINT())
                                        .column("log_c4", DataTypes.INT())
                                        .column("log_c5", DataTypes.BIGINT())
                                        .column("log_c6", DataTypes.FLOAT())
                                        .column("log_c7", DataTypes.DOUBLE())
                                        .column("log_c8", DataTypes.DECIMAL(10, 2))
                                        .column("log_c9", DataTypes.CHAR(10))
                                        .column("log_c10", DataTypes.STRING())
                                        .column("log_c11", DataTypes.BYTES())
                                        .column("log_c12", DataTypes.BINARY(5))
                                        .column("log_c13", DataTypes.DATE())
                                        .column("log_c14", DataTypes.TIME())
                                        .column("log_c15", DataTypes.TIMESTAMP())
                                        .column("log_c16", DataTypes.TIMESTAMP_LTZ())
                                        .build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();
        TablePath tablePath = TablePath.of("fluss", "all_types_table");
        flussPaimonCatalog.createTable(tablePath, tableDescriptor);

        TableDescriptor paimonDescriptor = flussPaimonCatalog.getTable(tablePath);

        verifyTableDescriptor(tableDescriptor, paimonDescriptor);
    }

    @Test
    void testAlterTableConfigs() throws Exception {
        String database = "test_alter_table_configs_db";
        String tableName = "test_alter_table_configs_table";
        TablePath tablePath = TablePath.of(database, tableName);
        Identifier identifier = Identifier.create(database, tableName);
        createTable(database, tableName);
        Table table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);

        // value should be null for key
        assertThat(table.options().get("key")).isEqualTo(null);

        // set the value for key
        flussPaimonCatalog.alterTable(tablePath, Arrays.asList(TableChange.set("key", "value")));

        table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        // we have set the value for key
        assertThat(table.options().get("fluss.key")).isEqualTo("value");

        // reset the value for key
        flussPaimonCatalog.alterTable(tablePath, Arrays.asList(TableChange.reset("key")));

        table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        // we have reset the value for key
        assertThat(table.options().get("fluss.key")).isEqualTo(null);
    }

    private void createTable(String database, String tableName) {
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

        flussPaimonCatalog.createTable(tablePath, td);
    }

    private void verifyTableDescriptor(
            TableDescriptor tableDescriptor, TableDescriptor paimonDescriptor) {
        assertThat(tableDescriptor.getSchema()).isEqualTo(paimonDescriptor.getSchema());
        assertThat(tableDescriptor.getBucketKeys()).isEqualTo(paimonDescriptor.getBucketKeys());
        assertThat(tableDescriptor.getPartitionKeys())
                .isEqualTo(paimonDescriptor.getPartitionKeys());
        assertThat(tableDescriptor.getTableDistribution())
                .isEqualTo(paimonDescriptor.getTableDistribution());
        assertThat(tableDescriptor.getProperties()).isEqualTo(paimonDescriptor.getProperties());

        Map<String, String> customProperties = new HashMap<>(tableDescriptor.getCustomProperties());
        tableDescriptor
                .getProperties()
                .forEach(
                        (k, v) -> {
                            if (v.equals(customProperties.get(k))) {
                                customProperties.remove(k);
                            }
                        });
        Map<String, String> paimonCustomProperties =
                new HashMap<>(paimonDescriptor.getCustomProperties());
        paimonDescriptor
                .getProperties()
                .forEach(
                        (k, v) -> {
                            if (v.equals(paimonCustomProperties.get(k))) {
                                paimonCustomProperties.remove(k);
                            }
                        });
        assertThat(customProperties).isEqualTo(paimonCustomProperties);
    }
}
