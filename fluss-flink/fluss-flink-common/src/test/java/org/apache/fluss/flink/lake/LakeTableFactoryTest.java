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

package org.apache.fluss.flink.lake;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.flink.lake.LakeTableFactory.RESOLVED_LAKE_DATABASE;
import static org.apache.fluss.flink.lake.LakeTableFactory.RESOLVED_LAKE_OBJECT;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LakeTableFactory}. */
class LakeTableFactoryTest {

    @Test
    void testCreateLakeTableContextConsumesResolvedIdentifierOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "paimon");
        options.put(RESOLVED_LAKE_DATABASE, "custom_db");
        options.put(RESOLVED_LAKE_OBJECT, "custom_table$snapshots");
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder().build(), null, Collections.emptyList(), options);
        DynamicTableFactory.Context context =
                new FactoryUtil.DefaultDynamicTableContext(
                        ObjectIdentifier.of(
                                "fluss_catalog", "fluss_db", "fluss_table$lake$snapshots"),
                        new ResolvedCatalogTable(catalogTable, ResolvedSchema.of()),
                        Collections.emptyMap(),
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader(),
                        false);

        DynamicTableFactory.Context lakeContext = LakeTableFactory.createLakeTableContext(context);

        assertThat(lakeContext.getObjectIdentifier())
                .isEqualTo(
                        ObjectIdentifier.of(
                                "fluss_catalog", "custom_db", "custom_table$snapshots"));
        assertThat(lakeContext.getCatalogTable().getOptions())
                .containsEntry("connector", "paimon")
                .doesNotContainKeys(RESOLVED_LAKE_DATABASE, RESOLVED_LAKE_OBJECT);
        assertThat(context.getCatalogTable().getOptions())
                .containsKeys(RESOLVED_LAKE_DATABASE, RESOLVED_LAKE_OBJECT);
    }
}
