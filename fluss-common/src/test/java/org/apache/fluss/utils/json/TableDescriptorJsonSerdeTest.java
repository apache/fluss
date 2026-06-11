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

package org.apache.fluss.utils.json;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionKey;
import org.apache.fluss.metadata.TableDescriptor;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TableDescriptorJsonSerde}. */
public class TableDescriptorJsonSerdeTest extends JsonSerdeTestBase<TableDescriptor> {
    TableDescriptorJsonSerdeTest() {
        super(TableDescriptorJsonSerde.INSTANCE);
    }

    @Override
    protected TableDescriptor[] createObjects() {
        TableDescriptor[] tableDescriptors = new TableDescriptor[3];

        tableDescriptors[0] =
                TableDescriptor.builder()
                        .schema(SchemaJsonSerdeTest.SCHEMA_0)
                        .comment("first table")
                        .partitionedBy("c")
                        .distributedBy(16, "a")
                        .property("option-1", "100")
                        .property("option-2", "200")
                        .customProperties(Collections.singletonMap("custom-1", "\"value-1\""))
                        .build();

        tableDescriptors[1] =
                TableDescriptor.builder()
                        .schema(SchemaJsonSerdeTest.SCHEMA_1)
                        .distributedBy(32)
                        .property("option-3", "300")
                        .property("option-4", "400")
                        .logFormat(LogFormat.INDEXED)
                        .kvFormat(KvFormat.INDEXED)
                        .build();

        tableDescriptors[2] =
                TableDescriptor.builder()
                        .schema(SchemaJsonSerdeTest.SCHEMA_1)
                        .partitionedByKeys(
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "a_day",
                                                DateTruncPartitionTransform.of(
                                                        "a",
                                                        AutoPartitionTimeUnit.DAY,
                                                        ZoneId.of("UTC")))))
                        .distributedBy(16, "a")
                        .build();

        return tableDescriptors;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"schema\":"
                    + SchemaJsonSerdeTest.SCHEMA_JSON_0
                    + ",\"comment\":\"first table\",\"partition_key\":[\"c\"],\"bucket_key\":[\"a\"],\"bucket_count\":16,\"properties\":{\"option-2\":\"200\",\"option-1\":\"100\"},"
                    + "\"custom_properties\":{\"custom-1\":\"\\\"value-1\\\"\"}}",
            "{\"version\":1,\"schema\":"
                    + SchemaJsonSerdeTest.SCHEMA_JSON_1
                    + ",\"partition_key\":[],\"bucket_key\":[\"a\"],\"bucket_count\":32,\"properties\":{\"option-3\":\"300\",\"option-4\":\"400\","
                    + "\"table.log.format\":\"INDEXED\",\"table.kv.format\":\"INDEXED\"},\"custom_properties\":{}}",
            "{\"version\":1,\"schema\":"
                    + SchemaJsonSerdeTest.SCHEMA_JSON_1
                    + ",\"partition_key\":[\"a_day\"],\"partition_expressions\":[{\"virtual_partition_spec_key\":\"a_day\",\"transform\":{\"type\":\"date_trunc\",\"source_column\":\"a\",\"unit\":\"DAY\",\"time_zone\":\"UTC\"}}],\"bucket_key\":[\"a\"],\"bucket_count\":16,\"properties\":{},\"custom_properties\":{}}"
        };
    }

    @Test
    void testGeneratedVirtualKeyRoundTripsThroughJson() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(SchemaJsonSerdeTest.SCHEMA_1)
                        .partitionedByKeys(
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                DateTruncPartitionTransform.of(
                                                        "a", AutoPartitionTimeUnit.DAY))))
                        .distributedBy(16, "a")
                        .build();

        TableDescriptor roundTripped =
                JsonSerdeUtils.readValue(
                        JsonSerdeUtils.writeValueAsBytes(
                                descriptor, TableDescriptorJsonSerde.INSTANCE),
                        TableDescriptorJsonSerde.INSTANCE);

        assertThat(roundTripped.getPartitionKeys()).containsExactly("a_day");
        assertThat(roundTripped.getPartitionExpressions()).hasSize(1);
        assertThat(roundTripped.getPartitionExpressions().get(0).getVirtualPartitionSpecKey())
                .hasValue("a_day");
    }

    @Test
    void testRejectInvalidImplicitPartitionJson() {
        assertThatThrownBy(
                        () ->
                                readTableDescriptor(
                                        implicitPartitionJson(
                                                "[\"a_day\"]",
                                                "[{\"transform\":{\"type\":\"date_trunc\",\"source_column\":\"a\",\"unit\":\"DAY\"}}]")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Partition expression must contain virtual_partition_spec_key.");

        assertThatThrownBy(
                        () ->
                                readTableDescriptor(
                                        implicitPartitionJson(
                                                "[\"other_day\"]",
                                                "[{\"virtual_partition_spec_key\":\"a_day\",\"transform\":{\"type\":\"date_trunc\",\"source_column\":\"a\",\"unit\":\"DAY\"}}]")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Virtual partition spec key 'a_day' is not present in partition_key [other_day].");

        assertThatThrownBy(
                        () ->
                                readTableDescriptor(
                                        implicitPartitionJson(
                                                "[\"a_day\"]",
                                                "[{\"virtual_partition_spec_key\":\"a_day\",\"transform\":{\"type\":\"date_trunc\",\"source_column\":\"a\",\"unit\":\"DAY\"}},"
                                                        + "{\"virtual_partition_spec_key\":\"a_day\",\"transform\":{\"type\":\"date_trunc\",\"source_column\":\"a\",\"unit\":\"MONTH\"}}]")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate virtual partition spec key 'a_day'.");

        assertThatThrownBy(
                        () ->
                                readTableDescriptor(
                                        implicitPartitionJson(
                                                "[\"missing_day\",\"a_day\"]",
                                                "[{\"virtual_partition_spec_key\":\"a_day\",\"transform\":{\"type\":\"date_trunc\",\"source_column\":\"a\",\"unit\":\"DAY\"}}]")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Partition key 'missing_day' does not exist in the schema or partition expressions.");

        assertThatThrownBy(
                        () ->
                                readTableDescriptor(
                                        implicitPartitionJson(
                                                "[\"a_day\"]",
                                                "[{\"virtual_partition_spec_key\":\"a_day\",\"transform\":{\"type\":\"date_trunc\",\"source_column\":\"a\",\"unit\":\"DAY\",\"time_zone\":\"Invalid/Zone\"}}]")))
                .isInstanceOf(DateTimeException.class);
    }

    private static TableDescriptor readTableDescriptor(String json) {
        return JsonSerdeUtils.readValue(
                json.getBytes(StandardCharsets.UTF_8), TableDescriptorJsonSerde.INSTANCE);
    }

    private static String implicitPartitionJson(
            String partitionKeyJson, String partitionExpressionsJson) {
        return "{\"version\":1,\"schema\":"
                + SchemaJsonSerdeTest.SCHEMA_JSON_1
                + ",\"partition_key\":"
                + partitionKeyJson
                + ",\"partition_expressions\":"
                + partitionExpressionsJson
                + ",\"bucket_key\":[\"a\"],\"bucket_count\":16,\"properties\":{},\"custom_properties\":{}}";
    }
}
