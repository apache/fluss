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

    public DynamicTableSource createDynamicTableSource(
            DynamicTableFactory.Context context, String tableName) {
        ObjectIdentifier originIdentifier = context.getObjectIdentifier();
        ObjectIdentifier lakeIdentifier =
                ObjectIdentifier.of(
                        originIdentifier.getCatalogName(),
                        originIdentifier.getDatabaseName(),
                        tableName);
        DynamicTableFactory.Context newContext =
                new FactoryUtil.DefaultDynamicTableContext(
                        lakeIdentifier,
                        context.getCatalogTable(),
                        context.getEnrichmentOptions(),
                        context.getConfiguration(),
                        context.getClassLoader(),
                        context.isTemporary());

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

        // Get the appropriate factory based on connector type
        DynamicTableSourceFactory factory = getLakeTableFactory(connector);
        return factory.createDynamicTableSource(newContext);
    }

    private DynamicTableSourceFactory getLakeTableFactory(String connector) {
        if ("paimon".equalsIgnoreCase(connector)) {
            return getPaimonFactory();
        } else if ("iceberg".equalsIgnoreCase(connector)) {
            return getIcebergFactory();
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported lake connector: "
                            + connector
                            + ". Only 'paimon' and 'iceberg' are supported.");
        }
    }

    private DynamicTableSourceFactory getPaimonFactory() {
        try {
            Class<?> paimonFactoryClass =
                    Class.forName("org.apache.paimon.flink.FlinkTableFactory");
            return (DynamicTableSourceFactory)
                    paimonFactoryClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create Paimon table factory. Please ensure paimon-flink is on the classpath.",
                    e);
        }
    }

    private DynamicTableSourceFactory getIcebergFactory() {
        try {
            Class<?> icebergFactoryClass =
                    Class.forName("org.apache.iceberg.flink.FlinkDynamicTableFactory");
            return (DynamicTableSourceFactory)
                    icebergFactoryClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create Iceberg table factory. Please ensure iceberg-flink-runtime is on the classpath.",
                    e);
        }
    }
}
