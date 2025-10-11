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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.utils.DataLakeUtils;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.TableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import static org.apache.fluss.metadata.DataLakeFormat.ICEBERG;

/** A lake catalog to delegate the operations on lake table. */
public class LakeCatalogWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(LakeCatalogWrapper.class);

    private static final Map<String, CatalogFactory> CATALOG_FACTORY_MAP = new HashMap<>();
    private static final Map<String, Catalog> LAKECATALOG_MAP = new HashMap<>();

    private final String catalogName;
    private final ClassLoader classLoader;

    public LakeCatalogWrapper(String catalogName, ClassLoader classLoader) {
        this.catalogName = catalogName;
        this.classLoader = classLoader;

        // Iceberg catalog factory is not supported by Factory spi, we need to discover it by
        // TableFactory.
        // Also, Iceberg factory not implement factoryIdentifier(), we need to use
        // requiredContext().get("type") instead.
        Stream.concat(
                        ServiceLoader.load(Factory.class).stream(),
                        ServiceLoader.load(TableFactory.class).stream())
                .filter(factory -> factory instanceof CatalogFactory)
                .map(factory -> (CatalogFactory) factory)
                .forEach(
                        factory -> {
                            try {
                                if (factory.factoryIdentifier() == null) {
                                    CATALOG_FACTORY_MAP.put(
                                            factory.requiredContext().get("type"), factory);
                                } else {
                                    CATALOG_FACTORY_MAP.put(factory.factoryIdentifier(), factory);
                                }
                            } catch (Exception e) {
                                LOG.warn(
                                        "Failed to load catalog factory: {}",
                                        factory.getClass().getName(),
                                        e);
                            }
                        });
    }

    public Catalog getOrCreateLakeCatalog(Configuration tableOptions) {
        String lakeFormat = tableOptions.get(ConfigOptions.TABLE_DATALAKE_FORMAT).toString();
        synchronized (this) {
            if (CATALOG_FACTORY_MAP.containsKey(lakeFormat)) {
                LAKECATALOG_MAP.computeIfAbsent(
                        lakeFormat,
                        k -> {
                            try {
                                Map<String, String> catalogProperties =
                                        DataLakeUtils.extractLakeCatalogProperties(tableOptions);
                                // cache lake catalog
                                if (lakeFormat.equals(ICEBERG.toString())) {
                                    // Iceberg catalog factory required "catalog-type" instead of
                                    // "type".
                                    catalogProperties.put(
                                            "catalog-type", catalogProperties.get("type"));
                                    // Iceberg catalog factory not implement `Catalog
                                    // createCatalog(Context context)` method, we need to use
                                    // `Catalog createCatalog(String name, Map<String, String>
                                    // properties)` instead.
                                    return CATALOG_FACTORY_MAP
                                            .get(lakeFormat)
                                            .createCatalog(ICEBERG.name(), catalogProperties);
                                }
                                DefaultCatalogContext catalogContext =
                                        new DefaultCatalogContext(
                                                catalogName,
                                                classLoader,
                                                catalogProperties,
                                                org.apache.flink.configuration.Configuration
                                                        .fromMap(catalogProperties));
                                return CATALOG_FACTORY_MAP
                                        .get(lakeFormat)
                                        .createCatalog(catalogContext);
                            } catch (Exception e) {
                                throw new FlussRuntimeException(
                                        String.format("Failed to init %s catalog.", lakeFormat), e);
                            }
                        });
                return LAKECATALOG_MAP.get(lakeFormat);
            } else {
                throw new UnsupportedOperationException(
                        "Not support datalake format: " + lakeFormat);
            }
        }
    }

    /** A default catalog context. */
    private static class DefaultCatalogContext implements CatalogFactory.Context {
        private String catalogName;
        private ClassLoader classLoader;
        private Map<String, String> options;
        private ReadableConfig configuration;

        public DefaultCatalogContext(
                String catalogName,
                ClassLoader classLoader,
                Map<String, String> options,
                ReadableConfig configuration) {
            this.catalogName = catalogName;
            this.classLoader = classLoader;
            this.options = options;
            this.configuration = configuration;
        }

        @Override
        public String getName() {
            return catalogName;
        }

        @Override
        public Map<String, String> getOptions() {
            return options;
        }

        @Override
        public ReadableConfig getConfiguration() {
            return configuration;
        }

        @Override
        public ClassLoader getClassLoader() {
            return classLoader;
        }
    }
}
