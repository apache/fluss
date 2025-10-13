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

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.catalog.FlinkCatalog;
import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;
import static org.apache.fluss.lake.iceberg.IcebergLakeCatalog.SYSTEM_COLUMNS;
import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link FlinkCatalog}. */
class FlinkCatalogLakeTest extends FlinkIcebergTieringTestBase {

    protected static final String DEFAULT_DB = "fluss";

    protected static final String CATALOG_NAME = "test_iceberg_lake";

    FlinkCatalog catalog;

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
        buildCatalog();
    }

    @Test
    // TODO: remove this test in #1803
    void testGetLakeTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(TABLE_DATALAKE_ENABLED.key(), "true");
        ObjectPath lakeTablePath = new ObjectPath(DEFAULT_DB, "lake_table");
        CatalogTable table = this.newCatalogTable(options);
        catalog.createTable(lakeTablePath, table, false);
        assertThat(catalog.tableExists(lakeTablePath)).isTrue();
        CatalogBaseTable lakeTable =
                catalog.getTable(new ObjectPath(DEFAULT_DB, "lake_table$lake"));
        Schema schema = lakeTable.getUnresolvedSchema();
        assertThat(schema.getColumns().size()).isEqualTo(3 + SYSTEM_COLUMNS.size());
        assertThat(schema.getPrimaryKey().isPresent()).isTrue();
        assertThat(schema.getPrimaryKey().get().getColumnNames()).isEqualTo(List.of("first"));
    }

    private CatalogTable newCatalogTable(Map<String, String> options) {
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("first", DataTypes.STRING().notNull()),
                                Column.physical("second", DataTypes.INT()),
                                Column.physical("third", DataTypes.STRING().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("PK_first", List.of("first")));
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
