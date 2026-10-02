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

package org.apache.fluss.metadata;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LakeTableUtilTest {

    @Test
    void testResolveLakeTablePath() {
        TablePath flussTablePath = TablePath.of("fluss_db", "fluss_table");
        Configuration tableConfig = new Configuration();

        assertThat(LakeTableUtil.resolveLakeTablePath(flussTablePath, tableConfig))
                .isEqualTo(flussTablePath);

        tableConfig.setString(ConfigOptions.TABLE_DATALAKE_DATABASE_NAME, "lake_db");
        assertThat(LakeTableUtil.resolveLakeTablePath(flussTablePath, tableConfig))
                .isEqualTo(TablePath.of("lake_db", "fluss_table"));

        tableConfig.setString(ConfigOptions.TABLE_DATALAKE_TABLE_NAME, "lake_table");
        assertThat(LakeTableUtil.resolveLakeTablePath(flussTablePath, tableConfig))
                .isEqualTo(TablePath.of("lake_db", "lake_table"));

        tableConfig.setString(ConfigOptions.TABLE_DATALAKE_DATABASE_NAME, "fluss_db");
        tableConfig.setString(ConfigOptions.TABLE_DATALAKE_TABLE_NAME, "fluss_table");
        assertThat(LakeTableUtil.resolveLakeTablePath(flussTablePath, tableConfig))
                .isEqualTo(flussTablePath);
    }

    @Test
    void testIsLakeTablePathChange() {
        String databaseNameKey = ConfigOptions.TABLE_DATALAKE_DATABASE_NAME.key();
        String tableNameKey = ConfigOptions.TABLE_DATALAKE_TABLE_NAME.key();

        assertThat(LakeTableUtil.isLakeTablePathChange(TableChange.set(databaseNameKey, "db")))
                .isTrue();
        assertThat(LakeTableUtil.isLakeTablePathChange(TableChange.reset(databaseNameKey)))
                .isTrue();
        assertThat(LakeTableUtil.isLakeTablePathChange(TableChange.set(tableNameKey, "table")))
                .isTrue();
        assertThat(LakeTableUtil.isLakeTablePathChange(TableChange.reset(tableNameKey))).isTrue();
        assertThat(
                        LakeTableUtil.isLakeTablePathChange(
                                TableChange.set(
                                        ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")))
                .isFalse();
        assertThat(LakeTableUtil.isLakeTablePathChange(TableChange.dropColumn("c1"))).isFalse();
    }
}
