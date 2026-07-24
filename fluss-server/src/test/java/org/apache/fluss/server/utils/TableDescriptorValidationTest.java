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

package org.apache.fluss.server.utils;

import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidDatabaseException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableDescriptorValidationTest {

    @Test
    void testRejectInvalidLakeDatabaseName() {
        TableDescriptor tableDescriptor =
                tableDescriptorWithLakeName(
                        ConfigOptions.TABLE_DATALAKE_DATABASE_NAME, "../lake_db");

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, 100, DataLakeFormat.PAIMON))
                .isInstanceOf(InvalidDatabaseException.class)
                .hasMessageContaining("../lake_db");
    }

    @Test
    void testRejectInvalidLakeTableName() {
        TableDescriptor tableDescriptor =
                tableDescriptorWithLakeName(
                        ConfigOptions.TABLE_DATALAKE_TABLE_NAME, "/tmp/lake_table");

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, 100, DataLakeFormat.PAIMON))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("/tmp/lake_table");
    }

    @Test
    void testAcceptValidLakeDatabaseAndTableNames() {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_DATALAKE_DATABASE_NAME, "lake_db-1")
                        .property(ConfigOptions.TABLE_DATALAKE_TABLE_NAME, "lake_table-1")
                        .build();

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, 100, DataLakeFormat.PAIMON))
                .doesNotThrowAnyException();
    }

    private static TableDescriptor tableDescriptorWithLakeName(
            ConfigOption<String> configOption, String lakeName) {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                .distributedBy(1)
                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                .property(configOption, lakeName)
                .build();
    }
}
