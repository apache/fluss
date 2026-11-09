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

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.lake.LakeFlinkCatalog;

import org.apache.iceberg.flink.FlinkCatalog;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LakeFlinkCatalog} with Iceberg. */
class LakeFlinkCatalogTest {

    @TempDir private Path warehouse;

    @ParameterizedTest
    @CsvSource(
            value = {"fluss_catalog, fluss_catalog", "NULL, fluss-iceberg-catalog"},
            nullValues = "NULL")
    void testIcebergCatalogNamedLikeServerCatalog(
            @Nullable String configuredName, String expectedName) throws Exception {
        Configuration tableOptions = new Configuration();
        tableOptions.setString("table.datalake.format", "iceberg");
        tableOptions.setString("table.datalake.iceberg.type", "hadoop");
        tableOptions.setString("table.datalake.iceberg.warehouse", warehouse.toString());
        if (configuredName != null) {
            tableOptions.setString("table.datalake.iceberg.name", configuredName);
        }

        try (LakeFlinkCatalog lakeFlinkCatalog =
                new LakeFlinkCatalog(
                        "my_flink_catalog", Thread.currentThread().getContextClassLoader())) {
            FlinkCatalog icebergCatalog =
                    (FlinkCatalog)
                            lakeFlinkCatalog.getLakeCatalog(tableOptions, Collections.emptyMap());

            assertThat(icebergCatalog.catalog().name()).isEqualTo(expectedName);
        }
    }
}
