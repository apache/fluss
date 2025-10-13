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
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.metadata.DataLakeFormat.ICEBERG;
import static org.apache.fluss.metadata.DataLakeFormat.PAIMON;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/** A lake catalog to delegate the operations on lake table. */
public class LakeCatalog {
    private static final Map<DataLakeFormat, Catalog> LAKE_CATALOG_CACHE =
            MapUtils.newConcurrentHashMap();

    private static final ReentrantLock LOCK = new ReentrantLock();

    private final String catalogName;
    private final ClassLoader classLoader;

    public LakeCatalog(String catalogName, ClassLoader classLoader) {
        this.catalogName = catalogName;
        this.classLoader = classLoader;
    }

    public Catalog getLakeCatalog(Configuration tableOptions) {
        return inLock(
                LOCK,
                () -> {
                    DataLakeFormat lakeFormat =
                            tableOptions.get(ConfigOptions.TABLE_DATALAKE_FORMAT);
                    if (lakeFormat == PAIMON) {
                        // TODO: Currently, a Fluss cluster only supports a single DataLake storage.
                        // However, in the
                        //  future, it may support multiple DataLakes. The following code assumes
                        // that a single
                        //  lakeCatalog is shared across multiple tables, which will no longer be
                        // valid in such
                        //  cases and should be updated accordingly.
                        LAKE_CATALOG_CACHE.computeIfAbsent(
                                PAIMON,
                                k ->
                                        PaimonCatalogFactory.create(
                                                catalogName, tableOptions, classLoader));

                    } else if (lakeFormat == ICEBERG) {
                        LAKE_CATALOG_CACHE.computeIfAbsent(
                                ICEBERG,
                                k -> IcebergCatalogFactory.create(catalogName, tableOptions));
                    } else {
                        throw new UnsupportedOperationException(
                                "Unsupported datalake format: " + lakeFormat);
                    }
                    return LAKE_CATALOG_CACHE.get(lakeFormat);
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

        // Iceberg 1.4.3 is the last Java 8 compatible version, while Flink Iceberg 1.18+ requires
        // 1.5.0+.
        // Using reflection to maintain Java 8 compatibility.
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
