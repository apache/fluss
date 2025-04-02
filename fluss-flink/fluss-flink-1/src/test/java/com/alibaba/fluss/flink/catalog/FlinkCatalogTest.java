/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.catalog;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;

import org.junit.jupiter.api.BeforeAll;

/** Test for {@link FlinkCatalog}. */
class FlinkCatalogTest extends CatalogTestBase {

    @BeforeAll
    static void beforeAll() {
        // set fluss conf
        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        catalog =
                new FlinkCatalog(
                        CATALOG_NAME,
                        DEFAULT_DB,
                        String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS)),
                        Thread.currentThread().getContextClassLoader());
        catalog.open();
    }

    @Override
    public CatalogTableFunction catalogTableFunction() {
        return FlinkCatalogTableFunction.INSTANCE;
    }
}
