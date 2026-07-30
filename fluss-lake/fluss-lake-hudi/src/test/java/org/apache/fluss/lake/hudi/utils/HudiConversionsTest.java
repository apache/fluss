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

package org.apache.fluss.lake.hudi.utils;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionKey;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.fluss.lake.hudi.utils.HudiConversions.FLUSS_BUCKET_AWARE_OPTION;
import static org.apache.fluss.lake.hudi.utils.HudiConversions.FLUSS_BUCKET_KEYS_OPTION;
import static org.apache.fluss.lake.hudi.utils.HudiConversions.FLUSS_PARTITION_KEYS_OPTION;
import static org.apache.fluss.lake.hudi.utils.catalog.HudiCatalogUtils.FILE_SYSTEM_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HudiConversions}. */
class HudiConversionsTest {

    @Test
    void testBuildHudiTablePropertiesCarriesFlussBucketMetadataForPkTable() {
        Schema schema =
                Schema.newBuilder().column("id", DataTypes.BIGINT()).primaryKey("id").build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        Map<String, String> properties =
                HudiConversions.buildHudiTableProperties(
                        TablePath.of("db1", "table1"), tableDescriptor, true);

        assertThat(properties).containsEntry(FLUSS_BUCKET_AWARE_OPTION, "true");
        assertThat(properties).containsEntry(FLUSS_BUCKET_KEYS_OPTION, "id");
        assertThat(properties).containsEntry(FLUSS_PARTITION_KEYS_OPTION, "");
    }

    @Test
    void testBuildHudiTablePropertiesCarriesFlussBucketMetadataForBucketUnawareLogTable() {
        Schema schema = Schema.newBuilder().column("id", DataTypes.BIGINT()).build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3)
                        .property("hudi.hoodie.datasource.write.recordkey.field", "id")
                        .build();

        Map<String, String> properties =
                HudiConversions.buildHudiTableProperties(
                        TablePath.of("db1", "table1"), tableDescriptor, false);

        assertThat(properties).containsEntry(FLUSS_BUCKET_AWARE_OPTION, "false");
        assertThat(properties).containsEntry(FLUSS_BUCKET_KEYS_OPTION, "");
        assertThat(properties).containsEntry(FLUSS_PARTITION_KEYS_OPTION, "");
    }

    @Test
    void testBuildHudiTablePropertiesCarriesFlussPartitionMetadata() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("dt", DataTypes.STRING())
                        .column("hr", DataTypes.STRING())
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .partitionedBy("dt", "hr")
                        .property("hudi.hoodie.datasource.write.recordkey.field", "id")
                        .build();

        Map<String, String> properties =
                HudiConversions.buildHudiTableProperties(
                        TablePath.of("db1", "table1"), tableDescriptor, false);

        assertThat(properties).containsEntry(FLUSS_PARTITION_KEYS_OPTION, "dt,hr");
    }

    @Test
    void testCreateHudiCatalogTableRejectsImplicitPartition() {
        assertThatThrownBy(
                        () ->
                                HudiConversions.createHudiCatalogTable(
                                        TablePath.of("test_db", "implicit_partition_table"),
                                        implicitPartitionDescriptor(),
                                        false,
                                        FILE_SYSTEM_TYPE))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Hudi lake tables do not support implicit partition expressions yet.");
    }

    private static TableDescriptor implicitPartitionDescriptor() {
        Schema schema =
                Schema.newBuilder()
                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                        .column("payload", DataTypes.STRING())
                        .build();
        return TableDescriptor.builder()
                .schema(schema)
                .partitionedByKeys(
                        PartitionKey.expression(
                                PartitionExpression.of(
                                        "event_day",
                                        DateTruncPartitionTransform.of(
                                                "event_time", AutoPartitionTimeUnit.DAY))))
                .distributedBy(1)
                .build();
    }
}
