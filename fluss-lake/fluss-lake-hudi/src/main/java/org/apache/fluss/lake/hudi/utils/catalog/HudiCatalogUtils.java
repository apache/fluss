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

package org.apache.fluss.lake.hudi.utils.catalog;

import org.apache.fluss.config.Configuration;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.hudi.exception.HoodieCatalogException;
import org.apache.hudi.table.catalog.CatalogOptions;
import org.apache.hudi.table.catalog.HoodieCatalog;
import org.apache.hudi.table.catalog.HoodieHiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Implementation of {@link HudiCatalogUtils} for Hudi. */
public class HudiCatalogUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HudiCatalogUtils.class);

    private static final String CATALOG_NAME_KEY = "name";

    public static final String HUDI_CATALOG_DEFAULT_NAME = "fluss-hudi-catalog";
    public static final String HIVE_META_STORE_TYPE = "hms";
    public static final String FILE_SYSTEM_TYPE = "dfs";

    public static Catalog createHudiCatalog(Configuration configuration) {
        Map<String, String> hudiProps = configuration.toMap();
        // copy the configuration to avoid modifying the original
        Configuration copiedConfig = new Configuration(configuration);
        copiedConfig.setString(CatalogOptions.DEFAULT_DATABASE.key(), "tmp");
        copiedConfig.setString(CatalogOptions.TABLE_EXTERNAL.key(), "true");
        String catalogName = hudiProps.getOrDefault(CATALOG_NAME_KEY, HUDI_CATALOG_DEFAULT_NAME);
        return buildHudiCatalog(
                catalogName,
                hudiProps,
                org.apache.flink.configuration.Configuration.fromMap(copiedConfig.toMap()));
    }

    public static Catalog buildHudiCatalog(
            String catalogName,
            Map<String, String> hudiProps,
            org.apache.flink.configuration.Configuration configuration) {
        String catalogMode =
                hudiProps.getOrDefault(CatalogOptions.MODE.key(), HIVE_META_STORE_TYPE);
        LOG.info("create hudi catalog: {}, mode: {}", catalogName, catalogMode);
        switch (catalogMode.toLowerCase(Locale.ENGLISH)) {
            case HIVE_META_STORE_TYPE:
                return new HoodieHiveCatalog(catalogName, configuration);
            case FILE_SYSTEM_TYPE:
                return new HoodieCatalog(catalogName, configuration);
            default:
                throw new HoodieCatalogException(
                        String.format(
                                "Invalid hudi-catalog mode: %s, supported modes: [hms, dfs].",
                                catalogMode));
        }
    }

    public static CatalogTable createCatalogTable(
            Schema schema,
            List<String> partitionKeys,
            Map<String, String> options,
            @Nullable String comment) {
        return CatalogTable.of(schema, comment, partitionKeys, options);
    }

    public static ResolvedCatalogTable createResolvedCatalogTable(
            Schema schema,
            List<String> partitionKeys,
            Map<String, String> options,
            @Nullable String comment,
            ResolvedSchema resolvedSchema) {
        return new ResolvedCatalogTable(
                CatalogTable.of(schema, comment, partitionKeys, options), resolvedSchema);
    }
}
