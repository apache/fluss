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

import org.apache.iceberg.catalog.Catalog;

import java.util.Map;

import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;

/** Iceberg catalog utils. */
public class IcebergCatalogUtils {

    public static final String ICEBERG_CATALOG_DEFAULT_NAME = "fluss-iceberg-catalog";

    public static Catalog createIcebergCatalog(Configuration configuration) {
        Map<String, String> icebergProps = configuration.toMap();
        String catalogName = icebergProps.getOrDefault("name", ICEBERG_CATALOG_DEFAULT_NAME);
        return buildIcebergCatalog(
                catalogName, icebergProps, IcebergConfiguration.from(configuration).get());
    }
}
