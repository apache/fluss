/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.flink;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.catalog.FlinkCatalog;
import org.apache.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;
import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_FORMAT;
import static org.apache.fluss.lake.paimon.PaimonLakeCatalog.SYSTEM_COLUMNS;
import static org.apache.fluss.metadata.DataLakeFormat.PAIMON;
import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link FlinkCatalog}. */
public class FlinkCatalogLakeTest extends FlinkPaimonTieringTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    FlinkCatalog catalog;

    @Override
    protected FlussClusterExtension getFlussClusterExtension() {
        return FLUSS_CLUSTER_EXTENSION;
    }

    @BeforeAll
    protected static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll(FLUSS_CLUSTER_EXTENSION.getClientConfig());
    }

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
        buildCatalog();
    }

    @Test
    // TODO: duplicate code in paimon and iceberg, refactor it after #1709
    void testGetLakeTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(TABLE_DATALAKE_ENABLED.key(), "true");
        options.put(TABLE_DATALAKE_FORMAT.key(), PAIMON.name());
        String warehousePath =
                Files.createTempDirectory("fluss-testing-datalake-tiered")
                        .resolve("warehouse")
                        .toString();
        options.put("datalake.paimon.warehouse", warehousePath);

        ObjectPath lakeTablePath = new ObjectPath(DEFAULT_DB, "lake_table");
        CatalogTable table = this.newCatalogTable(options);
        catalog.createTable(lakeTablePath, table, false);
        assertThat(catalog.tableExists(lakeTablePath)).isTrue();
        CatalogBaseTable lakeTable =
                catalog.getTable(new ObjectPath(DEFAULT_DB, "lake_table$lake"));
        Schema schema = lakeTable.getUnresolvedSchema();
        assertThat(schema.getColumns().size()).isEqualTo(3 + SYSTEM_COLUMNS.size());
        assertThat(schema.getPrimaryKey().isPresent()).isTrue();
        assertThat(schema.getPrimaryKey().get().getColumnNames())
                .isEqualTo(Arrays.asList("first", "third"));
    }

    private CatalogTable newCatalogTable(Map<String, String> options) {
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("first", DataTypes.STRING().notNull()),
                                Column.physical("second", DataTypes.INT()),
                                Column.physical("third", DataTypes.STRING().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey(
                                "PK_first_third", Arrays.asList("first", "third")));
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        options);
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    public void buildCatalog() {
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        catalog =
                new FlinkCatalog(
                        CATALOG_NAME,
                        DEFAULT_DB,
                        bootstrapServers,
                        Thread.currentThread().getContextClassLoader(),
                        Collections.emptyMap());
        catalog.open();
    }
}
