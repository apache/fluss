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

import org.apache.fluss.config.Configuration;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;

/** A factory to create {@link DynamicTableSource} for lake table. */
public class LakeTableFactory {
    private final LakeFlinkCatalog lakeFlinkCatalog;

    public LakeTableFactory(LakeFlinkCatalog lakeFlinkCatalog) {
        this.lakeFlinkCatalog = lakeFlinkCatalog;
    }

    public DynamicTableSource createDynamicTableSource(
            DynamicTableFactory.Context context, String tableName) {
        ObjectIdentifier originIdentifier = context.getObjectIdentifier();
        ObjectIdentifier lakeIdentifier =
                ObjectIdentifier.of(
                        originIdentifier.getCatalogName(),
                        originIdentifier.getDatabaseName(),
                        tableName);

        // For Iceberg and Paimon, pass the table name as-is to their factory.
        // Metadata tables will be handled internally by their respective factories.
        DynamicTableFactory.Context newContext =
                new FactoryUtil.DefaultDynamicTableContext(
                        lakeIdentifier,
                        context.getCatalogTable(),
                        context.getEnrichmentOptions(),
                        context.getConfiguration(),
                        context.getClassLoader(),
                        context.isTemporary());

        // Get the appropriate factory based on connector type
        DynamicTableSourceFactory factory = getLakeTableFactory();
        return factory.createDynamicTableSource(newContext);
    }

    private DynamicTableSourceFactory getLakeTableFactory() {
        switch (lakeFlinkCatalog.getLakeFormat()) {
            case PAIMON:
                return getPaimonFactory();
            case ICEBERG:
                return getIcebergFactory();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported lake connector: "
                                + lakeFlinkCatalog.getLakeFormat()
                                + ". Only 'paimon' and 'iceberg' are supported.");
        }
    }

    private DynamicTableSourceFactory getPaimonFactory() {
        return new org.apache.paimon.flink.FlinkTableFactory();
    }

    private DynamicTableSourceFactory getIcebergFactory() {
        try {
            // Get catalog with explicit ICEBERG format
            org.apache.flink.table.catalog.Catalog catalog =
                    lakeFlinkCatalog.getLakeCatalog(
                            // we can pass empty configuration to get catalog
                            // since the catalog should already be initialized
                            new Configuration(), Collections.emptyMap());

            // Create FlinkDynamicTableFactory with the catalog
            Class<?> icebergFactoryClass =
                    Class.forName("org.apache.iceberg.flink.FlinkDynamicTableFactory");
            Class<?> flinkCatalogClass = Class.forName("org.apache.iceberg.flink.FlinkCatalog");
            return (DynamicTableSourceFactory)
                    icebergFactoryClass
                            .getDeclaredConstructor(flinkCatalogClass)
                            .newInstance(catalog);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create Iceberg table factory. Please ensure iceberg-flink-runtime is on the classpath.",
                    e);
        }
    }
}
