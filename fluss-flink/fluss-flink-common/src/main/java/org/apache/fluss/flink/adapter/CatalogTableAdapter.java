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

package org.apache.fluss.flink.adapter;

import org.apache.fluss.metadata.TableDescriptor;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A adapter for {@link CatalogTable} constructor, and adapter MATERIALIZED_TABLE for {@link
 * CatalogBaseTable.TableKind} TODO: remove this class when no longer support flink 1.18 and 1.19.
 */
public class CatalogTableAdapter {
    public static CatalogTable toCatalogTable(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options) {
        return CatalogTable.newBuilder()
                .schema(schema)
                .comment(comment)
                .partitionKeys(partitionKeys)
                .options(options)
                .build();
    }

    public static boolean isMaterializedTable(CatalogBaseTable.TableKind tableKind) {
        return CatalogBaseTable.TableKind.MATERIALIZED_TABLE == tableKind;
    }

    /**
     * Converts the {@code DISTRIBUTED BY} clause of a table or materialized table to a Fluss
     * distribution.
     *
     * @return the declared bucket keys and bucket count, or empty if the clause is absent
     * @throws CatalogException if the clause uses range distribution, which Fluss doesn't support
     */
    public static Optional<TableDescriptor.TableDistribution> toFlussDistribution(
            CatalogBaseTable table) {
        Optional<TableDistribution> distribution =
                isMaterializedTable(table.getTableKind())
                        ? CatalogMaterializedTableAdapter.getDistribution(
                                (CatalogMaterializedTable) table)
                        : ((CatalogTable) table).getDistribution();
        return distribution.map(
                d -> {
                    if (d.getKind() == TableDistribution.Kind.RANGE) {
                        throw new CatalogException(
                                "Fluss only supports hash distribution, but got "
                                        + "DISTRIBUTED BY RANGE.");
                    }
                    return new TableDescriptor.TableDistribution(
                            d.getBucketCount().orElse(null), d.getBucketKeys());
                });
    }
}
