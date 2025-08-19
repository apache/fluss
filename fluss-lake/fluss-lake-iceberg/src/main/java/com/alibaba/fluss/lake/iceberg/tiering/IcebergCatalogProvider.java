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

package com.alibaba.fluss.lake.iceberg.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.iceberg.conf.IcebergConfiguration;

import org.apache.iceberg.catalog.Catalog;

import java.io.Serializable;
import java.util.Map;

import static com.alibaba.fluss.lake.iceberg.IcebergLakeCatalog.ICEBERG_CATALOG_DEFAULT_NAME;
import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;

/** A provider for Iceberg catalog. */
public class IcebergCatalogProvider implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Configuration icebergConfig;

    public IcebergCatalogProvider(Configuration icebergConfig) {
        this.icebergConfig = icebergConfig;
    }

    public Catalog get() {
        Map<String, String> icebergProps = icebergConfig.toMap();
        String catalogName = icebergProps.getOrDefault("name", ICEBERG_CATALOG_DEFAULT_NAME);

        return buildIcebergCatalog(
                catalogName, icebergProps, IcebergConfiguration.from(icebergConfig).get());
    }
}
