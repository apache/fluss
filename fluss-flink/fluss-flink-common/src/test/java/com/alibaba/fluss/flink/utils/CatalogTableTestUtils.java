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

package com.alibaba.fluss.flink.utils;

import org.apache.flink.table.catalog.CatalogTable;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.config.ConfigOptions.TABLE_REPLICATION_FACTOR;
import static org.assertj.core.api.Assertions.assertThat;

/** Utils related to catalog table for test purpose. */
public class CatalogTableTestUtils {

    private CatalogTableTestUtils() {}

    /** create a table with a newly added options. */
    public static CatalogTable addOptions(
            CatalogTable catalogTable, Map<String, String> addedOptions) {
        Map<String, String> options = new HashMap<>(catalogTable.getOptions());
        options.putAll(addedOptions);
        return catalogTable.copy(options);
    }

    /**
     * Check the catalog table {@code actualTable} is equal to the catalog table {@code
     * expectedTable} without ignoring schema.
     */
    public static void checkEqualsRespectSchema(
            CatalogTable actualTable, CatalogTable expectedTable) {
        checkEquals(actualTable, expectedTable, false);
    }

    /**
     * Check the catalog table {@code actualTable} is equal to the catalog table {@code
     * expectedTable} with ignoring schema.
     */
    public static void checkEqualsIgnoreSchema(
            CatalogTable actualTable, CatalogTable expectedTable) {
        checkEquals(actualTable, expectedTable, true);
    }

    /**
     * Check the catalog table {@code actualTable} is equal to the catalog table {@code
     * expectedTable}.
     *
     * @param ignoreSchema whether to ignore schema
     */
    private static void checkEquals(
            CatalogTable actualTable, CatalogTable expectedTable, boolean ignoreSchema) {
        assertThat(actualTable.getTableKind()).isEqualTo(expectedTable.getTableKind());
        if (!ignoreSchema) {
            assertThat(actualTable.getUnresolvedSchema())
                    .isEqualTo(expectedTable.getUnresolvedSchema());
        }
        assertThat(actualTable.getComment()).isEqualTo(expectedTable.getComment());
        assertThat(actualTable.getPartitionKeys()).isEqualTo(expectedTable.getPartitionKeys());
        assertThat(actualTable.isPartitioned()).isEqualTo(expectedTable.isPartitioned());
        assertOptionsEqual(actualTable.getOptions(), expectedTable.getOptions());
    }

    private static void assertOptionsEqual(
            Map<String, String> actualOptions, Map<String, String> expectedOptions) {
        actualOptions.remove(BOOTSTRAP_SERVERS.key());
        actualOptions.remove(TABLE_REPLICATION_FACTOR.key());
        assertThat(actualOptions).isEqualTo(expectedOptions);
    }
}
