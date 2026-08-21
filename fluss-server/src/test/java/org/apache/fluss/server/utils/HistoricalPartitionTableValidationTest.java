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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HistoricalPartitionTableValidationTest {

    @Test
    void testReportsAllUnmetHistoricalPartitionRequirements() {
        // Case 1: Report disabled options, a missing format, and missing table keys together.
        TableDescriptor allRequirementsMissingDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED, true)
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        allRequirementsMissingDescriptor,
                                        100,
                                        DataLakeFormat.PAIMON))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessage(
                        "'table.datalake.historical-partition.enabled' has unmet requirements: "
                                + "'table.auto-partition.enabled' must be set to true; "
                                + "'table.datalake.enabled' must be set to true; "
                                + "'table.datalake.format' must be set to 'paimon' "
                                + "(currently not set); "
                                + "the table must define a primary key; "
                                + "the table must define exactly one partition key (found 0).");

        // Case 2: Aggregate requirements before related validators can report only one failure.
        TableDescriptor relatedValidationFailuresDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.ICEBERG)
                        .property(ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED, true)
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        relatedValidationFailuresDescriptor,
                                        100,
                                        DataLakeFormat.PAIMON))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessage(
                        "'table.datalake.historical-partition.enabled' has unmet requirements: "
                                + "'table.datalake.format' must be set to 'paimon' "
                                + "(currently 'iceberg'); "
                                + "the table must define a primary key; "
                                + "the table must define exactly one partition key (found 0).");
    }
}
