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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.utils.DataLakeUtils;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.utils.MapUtils;

import org.apache.flink.table.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkFileIOLoader;
import org.apache.paimon.options.Options;

import java.lang.reflect.Method;
import java.util.Map;

import static org.apache.fluss.metadata.DataLakeFormat.ICEBERG;
import static org.apache.fluss.metadata.DataLakeFormat.PAIMON;

/** A lake catalog to delegate the operations on lake table. */
public class LakeCatalog {
    private static final Map<DataLakeFormat, Catalog> LAKE_CATALOG_CACHE =
            MapUtils.newConcurrentHashMap();

    private final String catalogName;
    private final ClassLoader classLoader;

    public LakeCatalog(String catalogName, ClassLoader classLoader) {
        this.catalogName = catalogName;
        this.classLoader = classLoader;
    }

    public Catalog getLakeCatalog(Configuration tableOptions) {
        DataLakeFormat lakeFormat = tableOptions.get(ConfigOptions.TABLE_DATALAKE_FORMAT);
        if (lakeFormat == null) {
            throw new IllegalArgumentException(
                    "DataLake format is not specified in table options. "
                            + "Please ensure '"
                            + ConfigOptions.TABLE_DATALAKE_FORMAT.key()
                            + "' is set.");
        }
        return LAKE_CATALOG_CACHE.computeIfAbsent(
                lakeFormat,
                (dataLakeFormat) -> {
                    if (dataLakeFormat == PAIMON) {
                        return PaimonCatalogFactory.create(catalogName, tableOptions, classLoader);
                    } else if (dataLakeFormat == ICEBERG) {
                        return IcebergCatalogFactory.create(catalogName, tableOptions);
                    } else {
                        throw new UnsupportedOperationException(
                                "Unsupported datalake format: " + dataLakeFormat);
                    }
                });
    }

    public Catalog getLakeCatalog(Configuration tableOptions, DataLakeFormat lakeFormat) {
        if (lakeFormat == null) {
            throw new IllegalArgumentException("DataLake format cannot be null");
        }
        return LAKE_CATALOG_CACHE.computeIfAbsent(
                lakeFormat,
                (dataLakeFormat) -> {
                    if (dataLakeFormat == PAIMON) {
                        return PaimonCatalogFactory.create(catalogName, tableOptions, classLoader);
                    } else if (dataLakeFormat == ICEBERG) {
                        return IcebergCatalogFactory.create(catalogName, tableOptions);
                    } else {
                        throw new UnsupportedOperationException(
                                "Unsupported datalake format: " + dataLakeFormat);
                    }
                });
    }

    /**
     * Factory for creating Paimon Catalog instances.
     *
     * <p>Purpose: Encapsulates Paimon-related dependencies (e.g. FlinkFileIOLoader) to avoid direct
     * dependency in the main LakeCatalog class.
     */
    public static class PaimonCatalogFactory {

        private PaimonCatalogFactory() {}

        public static Catalog create(
                String catalogName, Configuration tableOptions, ClassLoader classLoader) {
            Map<String, String> catalogProperties =
                    DataLakeUtils.extractLakeCatalogProperties(tableOptions);
            return FlinkCatalogFactory.createCatalog(
                    catalogName,
                    CatalogContext.create(
                            Options.fromMap(catalogProperties), null, new FlinkFileIOLoader()),
                    classLoader);
        }
    }

    /** Factory use reflection to create Iceberg Catalog instances. */
    public static class IcebergCatalogFactory {

        private IcebergCatalogFactory() {}

        // Iceberg 1.4.3 is the last Java 8 compatible version, while the Flink 1.18+ connector
        // requires Iceberg 1.5.0+.
        // Using reflection to maintain Java 8 compatibility.
        // Once Fluss drops Java 8, we can remove the reflection code
        public static Catalog create(String catalogName, Configuration tableOptions) {
            Map<String, String> catalogProperties =
                    DataLakeUtils.extractLakeCatalogProperties(tableOptions);
            // Map "type" to "catalog-type" (equivalent)
            // Required: either "catalog-type" (standard type) or "catalog-impl"
            // (fully-qualified custom class, mandatory if "catalog-type" is missing)
            if (catalogProperties.containsKey("type")) {
                catalogProperties.put("catalog-type", catalogProperties.get("type"));
            }
            try {
                Class<?> flinkCatalogFactoryClass =
                        Class.forName("org.apache.iceberg.flink.FlinkCatalogFactory");
                Object factoryInstance =
                        flinkCatalogFactoryClass.getDeclaredConstructor().newInstance();

                Method createCatalogMethod =
                        flinkCatalogFactoryClass.getMethod(
                                "createCatalog", String.class, Map.class);
                return (Catalog)
                        createCatalogMethod.invoke(factoryInstance, catalogName, catalogProperties);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to create Iceberg catalog using reflection. Please make sure iceberg-flink-runtime is on the classpath.",
                        e);
            }
        }
    }
}
