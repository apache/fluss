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

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.conf.IcebergConfiguration;
import org.apache.fluss.utils.IOUtils;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.Map;

import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;

/** Iceberg catalog utils. */
public class IcebergCatalogUtils {

    public static final String ICEBERG_CATALOG_DEFAULT_NAME = "fluss-iceberg-catalog";

    // Iceberg 1.10 does not define RESTCatalogProperties; match the Iceberg REST spec constants.
    static final String SCAN_PLANNING_MODE = "scan-planning-mode";
    static final String SCAN_PLANNING_MODE_CLIENT = "client";
    static final String SCAN_PLANNING_MODE_SERVER = "server";

    public static Catalog createIcebergCatalog(Configuration configuration) {
        Map<String, String> icebergProps = configuration.toMap();
        String catalogName = icebergProps.getOrDefault("name", ICEBERG_CATALOG_DEFAULT_NAME);
        Catalog catalog =
                buildIcebergCatalog(
                        catalogName, icebergProps, IcebergConfiguration.from(configuration).get());
        try {
            validateCatalog(catalog);
        } catch (RuntimeException | Error e) {
            if (catalog instanceof AutoCloseable) {
                IOUtils.closeQuietly((AutoCloseable) catalog, catalogName);
            }
            throw e;
        }
        return catalog;
    }

    private static void validateCatalog(Catalog catalog) {
        if (!(catalog instanceof RESTCatalog)) {
            return;
        }
        RESTCatalog restCatalog = (RESTCatalog) catalog;
        Map<String, String> properties = restCatalog.properties();
        if (properties == null) {
            return;
        }
        String scanPlanningMode = properties.get(SCAN_PLANNING_MODE);
        if (scanPlanningMode == null
                || SCAN_PLANNING_MODE_CLIENT.equalsIgnoreCase(scanPlanningMode)) {
            return;
        }
        if (SCAN_PLANNING_MODE_SERVER.equalsIgnoreCase(scanPlanningMode)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Server-side scan planning is not supported by Fluss, but the effective "
                                    + "configuration of Iceberg REST catalog '%s' requires '%s=%s'. "
                                    + "If '%s' is a catalog default, set 'datalake.iceberg.%s: %s' in "
                                    + "server.yaml. If the REST catalog server enforces '%s' as an "
                                    + "override, change the server configuration to allow client-side "
                                    + "scan planning.",
                            restCatalog.name(),
                            SCAN_PLANNING_MODE,
                            scanPlanningMode,
                            SCAN_PLANNING_MODE_SERVER,
                            SCAN_PLANNING_MODE,
                            SCAN_PLANNING_MODE_CLIENT,
                            SCAN_PLANNING_MODE_SERVER));
        }
        throw new IllegalArgumentException(
                String.format(
                        "Invalid scan planning mode: %s. Valid values are: %s, %s",
                        scanPlanningMode, SCAN_PLANNING_MODE_CLIENT, SCAN_PLANNING_MODE_SERVER));
    }
}
