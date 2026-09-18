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

package org.apache.fluss.flink.catalog;

import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.testutils.common.MultiVersionTest;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.DefaultIndex;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkCatalog}. */
public class Flink23CatalogTest extends FlinkCatalogTest {

    protected ResolvedSchema createSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("first", DataTypes.STRING().notNull()),
                        Column.physical("second", DataTypes.INT()),
                        Column.physical("third", DataTypes.STRING().notNull())),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("PK_first_third", Arrays.asList("first", "third")),
                Collections.singletonList(
                        DefaultIndex.newIndex(
                                "INDEX_first_third", Arrays.asList("first", "third"))));
    }

    @Override
    protected ResolvedCatalogMaterializedTable createResolvedCatalogMaterializedTable(
            CatalogMaterializedTable origin,
            ResolvedSchema resolvedSchema,
            CatalogMaterializedTable.RefreshMode refreshMode,
            IntervalFreshness intervalFreshness) {
        return new ResolvedCatalogMaterializedTable(
                origin,
                resolvedSchema,
                refreshMode,
                intervalFreshness,
                StartMode.of(StartMode.StartModeKind.FROM_BEGINNING));
    }

    /**
     * Verifies that reading a legacy-shaped materialized-table payload (only {@code
     * definition-query} persisted, no {@code original-query}/{@code expanded-query}) succeeds under
     * Flink 2.3, where the underlying builder enforces non-null on the new fields. {@link
     * FlinkConversions} must fall back to {@code definition-query} for both missing fields.
     */
    @Test
    @MultiVersionTest
    void testMaterializedTableFallsBackToDefinitionQueryForLegacyData() {
        String definitionQuery = "SELECT order_id FROM t";

        Map<String, String> customProperties = new HashMap<>();
        customProperties.put(
                FlinkConnectorOptions.MATERIALIZED_TABLE_DEFINITION_QUERY.key(), definitionQuery);
        customProperties.put(
                FlinkConnectorOptions.MATERIALIZED_TABLE_INTERVAL_FRESHNESS.key(), "5");
        customProperties.put(
                FlinkConnectorOptions.MATERIALIZED_TABLE_INTERVAL_FRESHNESS_TIME_UNIT.key(),
                "SECOND");
        customProperties.put(
                FlinkConnectorOptions.MATERIALIZED_TABLE_LOGICAL_REFRESH_MODE.key(),
                CatalogMaterializedTable.LogicalRefreshMode.CONTINUOUS.name());
        customProperties.put(
                FlinkConnectorOptions.MATERIALIZED_TABLE_REFRESH_MODE.key(),
                CatalogMaterializedTable.RefreshMode.CONTINUOUS.name());
        customProperties.put(
                FlinkConnectorOptions.MATERIALIZED_TABLE_REFRESH_STATUS.key(),
                CatalogMaterializedTable.RefreshStatus.INITIALIZING.name());
        // Intentionally NO materialized-table.original-query / expanded-query entries —
        // simulates a legacy persisted payload from before Flink 2.3.

        org.apache.fluss.metadata.Schema flussSchema =
                org.apache.fluss.metadata.Schema.newBuilder()
                        .column("order_id", org.apache.fluss.types.DataTypes.STRING())
                        .build();
        TableDescriptor flussDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(1, Collections.singletonList("order_id"))
                        .customProperties(customProperties)
                        .build();

        TablePath tablePath = TablePath.of("db", "table");
        long currentMillis = System.currentTimeMillis();
        TableInfo tableInfo =
                TableInfo.of(
                        tablePath,
                        1L,
                        1,
                        flussDescriptor,
                        DEFAULT_REMOTE_DATA_DIR,
                        currentMillis,
                        currentMillis);

        CatalogMaterializedTable flinkTable =
                (CatalogMaterializedTable) FlinkConversions.toFlinkTable(tableInfo);

        assertThat(flinkTable.getDefinitionQuery()).isEqualTo(definitionQuery);
        // Flink 2.3 enforces non-null on original/expanded queries; verify the fallback
        // actually populated these fields with the definition-query value.
        assertThat(flinkTable.getOriginalQuery()).isEqualTo(definitionQuery);
        assertThat(flinkTable.getExpandedQuery()).isEqualTo(definitionQuery);
    }

    @Test
    @MultiVersionTest
    void testMaterializedTableSerializesSeparateQueriesIntoCustomProperties() {
        String originalQuery = "SELECT order_id, orig_ts FROM t";
        String expandedQuery = "SELECT default.t.order_id, default.t.orig_ts FROM default.t";

        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical(
                                        "order_id",
                                        org.apache.flink.table.api.DataTypes.STRING().notNull()),
                                Column.physical(
                                        "orig_ts",
                                        org.apache.flink.table.api.DataTypes.TIMESTAMP())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey(
                                "PK_order_id", Collections.singletonList("order_id")),
                        Collections.emptyList());
        Map<String, String> flinkOptions = new HashMap<>();
        flinkOptions.put("k1", "v1");

        CatalogMaterializedTable flinkMaterializedTable =
                CatalogMaterializedTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("test comment")
                        .options(flinkOptions)
                        .originalQuery(originalQuery)
                        .expandedQuery(expandedQuery)
                        .freshness(IntervalFreshness.ofSecond(5))
                        .logicalRefreshMode(CatalogMaterializedTable.LogicalRefreshMode.CONTINUOUS)
                        .refreshMode(CatalogMaterializedTable.RefreshMode.CONTINUOUS)
                        .refreshStatus(CatalogMaterializedTable.RefreshStatus.INITIALIZING)
                        .build();

        TableDescriptor flussTable =
                FlinkConversions.toFlussTable(
                        new ResolvedCatalogMaterializedTable(
                                flinkMaterializedTable,
                                resolvedSchema,
                                CatalogMaterializedTable.RefreshMode.CONTINUOUS,
                                IntervalFreshness.ofSecond(5),
                                StartMode.of(StartMode.StartModeKind.FROM_BEGINNING)));

        Map<String, String> customProperties = flussTable.getCustomProperties();

        assertThat(customProperties)
                .containsEntry(
                        FlinkConnectorOptions.MATERIALIZED_TABLE_ORIGINAL_QUERY.key(),
                        originalQuery);
        assertThat(customProperties)
                .containsEntry(
                        FlinkConnectorOptions.MATERIALIZED_TABLE_EXPANDED_QUERY.key(),
                        expandedQuery);

        // read back: the two queries must stay distinct across the round trip
        long currentMillis = System.currentTimeMillis();
        CatalogMaterializedTable readBack =
                (CatalogMaterializedTable)
                        FlinkConversions.toFlinkTable(
                                TableInfo.of(
                                        TablePath.of("db", "table"),
                                        1L,
                                        1,
                                        flussTable.withBucketCount(1),
                                        DEFAULT_REMOTE_DATA_DIR,
                                        currentMillis,
                                        currentMillis));
        assertThat(readBack.getOriginalQuery()).isEqualTo(originalQuery);
        assertThat(readBack.getExpandedQuery()).isEqualTo(expandedQuery);
    }
}
