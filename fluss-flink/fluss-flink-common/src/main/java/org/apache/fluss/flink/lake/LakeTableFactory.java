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

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A factory to create {@link DynamicTableSource} for lake table. */
public class LakeTableFactory {

    public static final String RESOLVED_LAKE_DATABASE = "fluss.internal.resolved-lake-database";
    public static final String RESOLVED_LAKE_OBJECT = "fluss.internal.resolved-lake-object";

    private final LakeFlinkCatalog lakeFlinkCatalog;

    public LakeTableFactory(LakeFlinkCatalog lakeFlinkCatalog) {
        this.lakeFlinkCatalog = lakeFlinkCatalog;
    }

    public DynamicTableSource createDynamicTableSource(DynamicTableFactory.Context context) {
        // For Iceberg and Paimon, pass the table name as-is to their factory.
        // Metadata tables will be handled internally by their respective factories.
        DynamicTableFactory.Context newContext = createLakeTableContext(context);

        // Get the appropriate factory based on connector type
        DynamicTableSourceFactory factory = getLakeTableFactory();
        return factory.createDynamicTableSource(newContext);
    }

    static DynamicTableFactory.Context createLakeTableContext(DynamicTableFactory.Context context) {
        Map<String, String> options = new HashMap<>(context.getCatalogTable().getOptions());
        String lakeDatabaseName =
                checkNotNull(
                        options.remove(RESOLVED_LAKE_DATABASE),
                        "Missing resolved lake database option.");
        String lakeObjectName =
                checkNotNull(
                        options.remove(RESOLVED_LAKE_OBJECT),
                        "Missing resolved lake object option.");
        ObjectIdentifier lakeIdentifier =
                ObjectIdentifier.of(
                        context.getObjectIdentifier().getCatalogName(),
                        lakeDatabaseName,
                        lakeObjectName);
        CatalogTable lakeTable = context.getCatalogTable().copy(options);
        ResolvedCatalogTable resolvedLakeTable =
                new ResolvedCatalogTable(lakeTable, context.getCatalogTable().getResolvedSchema());
        return new FactoryUtil.DefaultDynamicTableContext(
                lakeIdentifier,
                resolvedLakeTable,
                context.getEnrichmentOptions(),
                context.getConfiguration(),
                context.getClassLoader(),
                context.isTemporary());
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
