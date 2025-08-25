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

package com.alibaba.fluss.flink.lake;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.flink.utils.DataLakeUtils;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/** A lake catalog to delegate the operations on lake table. */
public class LakeCatalog {

    private static final Map<String, Catalog> LAKE_CATALOGS = new HashMap<>();
    private static final Map<String, CatalogFactory> CATALOG_FACTORY_MAP = new HashMap<>();

    private final String catalogName;
    private final ClassLoader classLoader;

    public LakeCatalog(String catalogName, ClassLoader classLoader) {
        this.catalogName = catalogName;
        this.classLoader = classLoader;

        for (CatalogFactory catalogFactory :
                ServiceLoader.load(CatalogFactory.class, classLoader)) {
            CATALOG_FACTORY_MAP.put(catalogFactory.factoryIdentifier(), catalogFactory);
        }
    }

    public Catalog getOrCreateLakeCatalog(Configuration tableOptions) {
        String lakeFormat = tableOptions.get(ConfigOptions.TABLE_DATALAKE_FORMAT).toString();
        synchronized (this) {
            if (CATALOG_FACTORY_MAP.containsKey(lakeFormat)) {
                if ((LAKE_CATALOGS.isEmpty() || !LAKE_CATALOGS.containsKey(lakeFormat))) {
                    try {
                        Map<String, String> catalogProperties =
                                DataLakeUtils.extractLakeCatalogProperties(tableOptions);
                        // cache lake catalog
                        DefaultCatalogContext catalogContext =
                                new DefaultCatalogContext(
                                        catalogName,
                                        classLoader,
                                        catalogProperties,
                                        org.apache.flink.configuration.Configuration.fromMap(
                                                catalogProperties));
                        LAKE_CATALOGS.put(
                                lakeFormat,
                                CATALOG_FACTORY_MAP.get(lakeFormat).createCatalog(catalogContext));
                    } catch (Exception e) {
                        throw new FlussRuntimeException(
                                String.format("Failed to init %s catalog.", lakeFormat), e);
                    }
                }
                return LAKE_CATALOGS.get(lakeFormat);
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
