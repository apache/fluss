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

package org.apache.fluss.flink.catalog;

import org.apache.fluss.flink.lake.LakeFlinkCatalog;
import org.apache.fluss.metadata.TableInfo;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A {@link FlinkCatalog} used for Flink 2.1. */
public class Flink21Catalog extends FlinkCatalog {

    public Flink21Catalog(
            String name,
            String defaultDatabase,
            String bootstrapServers,
            ClassLoader classLoader,
            Map<String, String> securityConfigs) {
        super(name, defaultDatabase, bootstrapServers, classLoader, securityConfigs);
    }

    @VisibleForTesting
    public Flink21Catalog(
            String name,
            String defaultDatabase,
            String bootstrapServers,
            ClassLoader classLoader,
            Map<String, String> securityConfigs,
            LakeFlinkCatalog lakeFlinkCatalog) {
        super(
                name,
                defaultDatabase,
                bootstrapServers,
                classLoader,
                securityConfigs,
                lakeFlinkCatalog);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath objectPath)
            throws TableNotExistException, CatalogException {
        CatalogBaseTable catalogBaseTable = super.getTable(objectPath);
        if (!(catalogBaseTable instanceof CatalogTable)
                || objectPath.getObjectName().contains(LAKE_TABLE_SPLITTER)) {
            return catalogBaseTable;
        }

        CatalogTable table = (CatalogTable) catalogBaseTable;
        Optional<Schema.UnresolvedPrimaryKey> pkOp = table.getUnresolvedSchema().getPrimaryKey();
        // If there is no pk, return directly.
        if (pkOp.isEmpty()) {
            return table;
        }

        Schema.Builder newSchemaBuilder =
                Schema.newBuilder().fromSchema(table.getUnresolvedSchema());
        // Pk is always an index.
        newSchemaBuilder.index(pkOp.get().getColumnNames());

        // Judge whether we can do prefix lookup.
        TableInfo tableInfo = connection.getTable(toTablePath(objectPath)).getTableInfo();
        List<String> bucketKeys = tableInfo.getBucketKeys();
        // For partition table, the physical primary key is the primary key that excludes the
        // partition key
        List<String> physicalPrimaryKeys = tableInfo.getPhysicalPrimaryKeys();
        List<String> indexKeys = new ArrayList<>();
        if (isPrefixList(physicalPrimaryKeys, bucketKeys)) {
            indexKeys.addAll(bucketKeys);
            if (tableInfo.isPartitioned()) {
                indexKeys.addAll(tableInfo.getPartitionKeys());
            }
        }

        if (!indexKeys.isEmpty()) {
            newSchemaBuilder.index(indexKeys);
        }
        return CatalogTable.newBuilder()
                .schema(newSchemaBuilder.build())
                .comment(table.getComment())
                .partitionKeys(table.getPartitionKeys())
                .options(table.getOptions())
                .snapshot(table.getSnapshot().orElse(null))
                .distribution(table.getDistribution().orElse(null))
                .build();
    }

    private static boolean isPrefixList(List<String> fullList, List<String> prefixList) {
        if (fullList.size() <= prefixList.size()) {
            return false;
        }

        for (int i = 0; i < prefixList.size(); i++) {
            if (!fullList.get(i).equals(prefixList.get(i))) {
                return false;
            }
        }
        return true;
    }
}
