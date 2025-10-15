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

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Map;

/** A factory to create {@link DynamicTableSource} for lake table. */
public class LakeTableFactory {
    private final LakeCatalog lakeCatalog;

    public LakeTableFactory(LakeCatalog lakeCatalog) {
        this.lakeCatalog = lakeCatalog;
    }

    public DynamicTableSource createDynamicTableSource(
            DynamicTableFactory.Context context, String tableName) {
        ObjectIdentifier originIdentifier = context.getObjectIdentifier();
        ObjectIdentifier lakeIdentifier =
                ObjectIdentifier.of(
                        originIdentifier.getCatalogName(),
                        originIdentifier.getDatabaseName(),
                        tableName);

        // Determine the lake format from the table options
        Map<String, String> tableOptions = context.getCatalogTable().getOptions();

        // If not present, fallback to 'fluss.table.datalake.format' (set by Fluss)
        String connector = tableOptions.get("connector");
        if (connector == null) {
            connector = tableOptions.get("fluss.table.datalake.format");
        }

        if (connector == null) {
            // For Paimon system tables (like table_name$options), the table options are empty
            // Default to Paimon for backward compatibility
            connector = "paimon";
        }

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
        DynamicTableSourceFactory factory = getLakeTableFactory(connector, tableOptions);
        return factory.createDynamicTableSource(newContext);
    }

    private DynamicTableSourceFactory getLakeTableFactory(
            String connector, Map<String, String> tableOptions) {
        if ("paimon".equalsIgnoreCase(connector)) {
            return getPaimonFactory();
        } else if ("iceberg".equalsIgnoreCase(connector)) {
            return getIcebergFactory(tableOptions);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported lake connector: "
                            + connector
                            + ". Only 'paimon' and 'iceberg' are supported.");
        }
    }

    private DynamicTableSourceFactory getPaimonFactory() {
        return new org.apache.paimon.flink.FlinkTableFactory();
    }

    private DynamicTableSourceFactory getIcebergFactory(Map<String, String> tableOptions) {
        try {
            // Get the Iceberg FlinkCatalog instance from LakeCatalog
            org.apache.fluss.config.Configuration flussConfig =
                    org.apache.fluss.config.Configuration.fromMap(tableOptions);

            // Get catalog with explicit ICEBERG format
            org.apache.flink.table.catalog.Catalog catalog =
                    lakeCatalog.getLakeCatalog(
                            flussConfig, org.apache.fluss.metadata.DataLakeFormat.ICEBERG);

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
