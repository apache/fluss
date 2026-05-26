
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
import org.apache.fluss.utils.PropertiesUtils;

import org.apache.flink.table.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkFileIOLoader;
import org.apache.paimon.options.Options;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.metadata.DataLakeFormat.HUDI;
import static org.apache.fluss.metadata.DataLakeFormat.ICEBERG;
import static org.apache.fluss.metadata.DataLakeFormat.PAIMON;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A lake flink catalog to delegate the operations on lake table. */
public class LakeFlinkCatalog implements AutoCloseable {

    private final String catalogName;
    private final ClassLoader classLoader;

    private volatile Catalog catalog;
    private volatile DataLakeFormat lakeFormat;

    public LakeFlinkCatalog(String catalogName, ClassLoader classLoader) {
        this.catalogName = catalogName;
        this.classLoader = classLoader;
    }

    public Catalog getLakeCatalog(
        Configuration tableOptions, Map<String, String> lakeCatalogProperties) {
        // TODO: Currently, a Fluss cluster only supports a single DataLake storage.
        // However, in the
        //  future, it may support multiple DataLakes. The following code assumes
        // that a single
        //  lakeCatalog is shared across multiple tables, which will no longer be
        // valid in such
        //  cases and should be updated accordingly.
        if (catalog == null) {
            synchronized (this) {
                if (catalog == null) {
                    DataLakeFormat lakeFormat =
                        tableOptions.get(ConfigOptions.TABLE_DATALAKE_FORMAT);
                    if (lakeFormat == null) {
                        throw new IllegalArgumentException(
                            "DataLake format is not specified in table options. "
                                + "Please ensure '"
                                + ConfigOptions.TABLE_DATALAKE_FORMAT.key()
                                + "' is set.");
                    }
                    Map<String, String> catalogProperties =
                        new HashMap<>(DataLakeUtils.extractLakeCatalogProperties(tableOptions));
                    // properties in catalog are preferred
                    catalogProperties.putAll(
                        PropertiesUtils.extractAndRemovePrefix(
                            lakeCatalogProperties, lakeFormat + "."));
                    if (lakeFormat == PAIMON) {
                        catalog =
                            PaimonCatalogFactory.create(
                                catalogName, catalogProperties, classLoader);
                        this.lakeFormat = PAIMON;
                    } else if (lakeFormat == ICEBERG) {
                        catalog = IcebergCatalogFactory.create(catalogName, catalogProperties);
                        this.lakeFormat = ICEBERG;
                    } else if (lakeFormat == HUDI) {
                        catalog =
                            HudiCatalogFactory.create(
                                catalogName, catalogProperties, classLoader);
                        this.lakeFormat = HUDI;
                    } else {
                        throw new UnsupportedOperationException(
                            "Unsupported data lake format: " + lakeFormat);
                    }
                }
            }
        }
        return catalog;
    }

    public DataLakeFormat getLakeFormat() {
        checkNotNull(
            lakeFormat,
            "DataLake format is null, must call getLakeCatalog first to initialize lake format.");
        return lakeFormat;
    }

    @Override
    public void close() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
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
            String catalogName,
            Map<String, String> catalogProperties,
            ClassLoader classLoader) {
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
        public static Catalog create(String catalogName, Map<String, String> catalogProperties) {
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

    /**
     * Factory using reflection to create Hudi Catalog instances.
     *
     * <p>Hudi is intentionally NOT a compile-time dependency of fluss-flink-common to avoid
     * dragging the shaded {@code hudi-flink<major>-bundle} (which re-bundles hadoop / parquet /
     * avro / jackson / guava) into every downstream consumer. The bundle is shipped as a plugin
     * under {@code plugins/hudi/} and loaded via the plugin classloader at runtime, mirroring the
     * Iceberg integration pattern.
     *
     * <p>Unlike Iceberg's {@code FlinkCatalogFactory}, which keeps a convenience overload {@code
     * createCatalog(String, Map)}, Hudi's {@code HoodieCatalogFactory} only exposes the standard
     * Flink SPI signature {@code createCatalog(CatalogFactory.Context)}. Since Fluss is not
     * invoking the factory through Flink's SQL {@code CREATE CATALOG} path, no {@code Context} is
     * provided to us — we have to build one ourselves. We do that by reflectively instantiating
     * Flink's {@code FactoryUtil$DefaultCatalogContext}, the same internal implementation Flink
     * itself uses in {@code FactoryUtil#createCatalog(...)} when bridging the legacy and new SPI
     * stacks.
     */
    public static class HudiCatalogFactory {

        private static final String HOODIE_CATALOG_FACTORY_CLASS =
            "org.apache.hudi.table.catalog.HoodieCatalogFactory";
        private static final String FLINK_CATALOG_FACTORY_CONTEXT_CLASS =
            "org.apache.flink.table.factories.CatalogFactory$Context";
        private static final String FLINK_DEFAULT_CATALOG_CONTEXT_CLASS =
            "org.apache.flink.table.factories.FactoryUtil$DefaultCatalogContext";

        private HudiCatalogFactory() {}

        public static Catalog create(
            String catalogName,
            Map<String, String> catalogProperties,
            ClassLoader classLoader) {
            try {
                // 1) Build Hudi's catalog factory instance via reflection.
                Class<?> hoodieCatalogFactoryClass =
                    Class.forName(HOODIE_CATALOG_FACTORY_CLASS, true, classLoader);
                Object factoryInstance =
                    hoodieCatalogFactoryClass.getDeclaredConstructor().newInstance();

                // 2) Build a CatalogFactory.Context via Flink's internal default impl.
                //    Constructor: DefaultCatalogContext(String name,
                //                                       Map<String,String> options,
                //                                       ReadableConfig configuration,
                //                                       ClassLoader classLoader)
                //    We have no real Flink ReadableConfig in this code path, so we pass an
                //    empty Flink Configuration (which implements ReadableConfig) as a benign
                //    placeholder — Hudi's factory only consumes 'options' / 'name' / classloader.
                Class<?> defaultCatalogContextClass =
                    Class.forName(FLINK_DEFAULT_CATALOG_CONTEXT_CLASS, true, classLoader);
                Class<?> readableConfigClass =
                    Class.forName(
                        "org.apache.flink.configuration.ReadableConfig", true, classLoader);
                Class<?> flinkConfigurationClass =
                    Class.forName(
                        "org.apache.flink.configuration.Configuration", true, classLoader);
                Object emptyFlinkConfiguration =
                    flinkConfigurationClass.getDeclaredConstructor().newInstance();

                Object context =
                    defaultCatalogContextClass
                        .getDeclaredConstructor(
                            String.class,
                            Map.class,
                            readableConfigClass,
                            ClassLoader.class)
                        .newInstance(
                            catalogName,
                            catalogProperties,
                            emptyFlinkConfiguration,
                            classLoader);

                // 3) Invoke HoodieCatalogFactory#createCatalog(Context).
                Class<?> contextInterface =
                    Class.forName(FLINK_CATALOG_FACTORY_CONTEXT_CLASS, true, classLoader);
                Method createCatalogMethod =
                    hoodieCatalogFactoryClass.getMethod("createCatalog", contextInterface);
                return (Catalog) createCatalogMethod.invoke(factoryInstance, context);
            } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to create Hudi catalog using reflection. Please make sure "
                        + "hudi-flink-bundle (matching the current Flink version, "
                        + "Hudi 1.0+) is on the classpath, typically under "
                        + "plugins/hudi/, and that the catalog options include a valid "
                        + "'mode' (supported: 'hms' or 'dfs').",
                    e);
            }
        }
    }
}
