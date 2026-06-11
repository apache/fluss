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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableDescriptor.TableDistribution;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestData;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Maps;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.json.JsonSerdeTestBase;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.server.zk.data.TableRegistrationJsonSerde}. */
class TableRegistrationJsonSerdeTest extends JsonSerdeTestBase<TableRegistration> {
    TableRegistrationJsonSerdeTest() {
        super(TableRegistrationJsonSerde.INSTANCE);
    }

    @Test
    void testInvalidTableRegistration() {
        // null bucket count
        assertThatThrownBy(
                        () ->
                                new TableRegistration(
                                        1234L,
                                        "first-table",
                                        Arrays.asList("a", "b"),
                                        new TableDistribution(null, Arrays.asList("b", "c")),
                                        Maps.newHashMap(),
                                        Collections.singletonMap("custom-3", "\"300\""),
                                        "file://local/remote",
                                        1735538268L,
                                        1735538270L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bucket count is required for table registration.");

        // null distribution
        assertThatThrownBy(
                        () ->
                                TableRegistration.newTable(
                                        11,
                                        "file://local/remote",
                                        TableDescriptor.builder()
                                                .schema(TestData.DATA1_SCHEMA)
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Table distribution is required for table registration.");
    }

    @Test
    void testLifecycleCopiesPreservePartitionExpressions() {
        TableRegistration registration = createObjects()[2];

        TableRegistration withNewProperties =
                registration.newProperties(
                        Collections.singletonMap("k", "v"),
                        Collections.singletonMap("custom-k", "custom-v"));
        assertThat(withNewProperties.partitionExpressions)
                .isEqualTo(registration.partitionExpressions);

        TableRegistration withNewRemoteDataDir =
                registration.newRemoteDataDir("file://local/other-remote");
        assertThat(withNewRemoteDataDir.partitionExpressions)
                .isEqualTo(registration.partitionExpressions);
    }

    @Test
    void testInvalidPartitionExpressions() {
        assertThatThrownBy(
                        () ->
                                tableRegistration(
                                        Collections.singletonList("event_day"),
                                        Collections.singletonList(
                                                PartitionExpression.of(
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Partition expression must contain a resolved virtual partition spec key.");

        assertThatThrownBy(
                        () ->
                                tableRegistration(
                                        Collections.singletonList("event_day"),
                                        Collections.singletonList(
                                                PartitionExpression.of(
                                                        "event_month",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.MONTH)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Virtual partition spec key 'event_month' is not present in partition keys [event_day].");

        assertThat(
                        tableRegistration(
                                        Collections.singletonList("event_day"),
                                        Collections.singletonList(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY))))
                                .partitionExpressions)
                .hasSize(1);

        assertThatThrownBy(
                        () ->
                                tableRegistration(
                                        Collections.singletonList("event_day"),
                                        Arrays.asList(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY,
                                                                ZoneId.of("UTC"))),
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.MONTH,
                                                                ZoneId.of("UTC"))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate virtual partition spec key 'event_day'.");
    }

    @Test
    void testAllowsZoneIndependentPersistedDateTrunc() {
        String json =
                "{\"version\":1,\"table_id\":1234,\"partition_key\":[\"event_day\"],"
                        + "\"partition_expressions\":[{\"virtual_partition_spec_key\":\"event_day\","
                        + "\"transform\":{\"type\":\"date_trunc\",\"source_column\":\"event_time\",\"unit\":\"DAY\"}}],"
                        + "\"bucket_count\":32,\"properties\":{},\"custom_properties\":{},"
                        + "\"created_time\":1735538268,\"modified_time\":1735538270}";

        TableRegistration registration =
                JsonSerdeUtils.readValue(
                        json.getBytes(StandardCharsets.UTF_8), TableRegistrationJsonSerde.INSTANCE);
        DateTruncPartitionTransform transform =
                (DateTruncPartitionTransform)
                        registration.partitionExpressions.get(0).getTransform();
        assertThat(transform.getTimeZone()).isEmpty();
    }

    @Test
    void testTimestampLtzPersistedTransformRequiresAndPreservesTimeZone() {
        String withoutTimeZone =
                "{\"version\":1,\"table_id\":1234,\"partition_key\":[\"event_day\"],"
                        + "\"partition_expressions\":[{\"virtual_partition_spec_key\":\"event_day\","
                        + "\"transform\":{\"type\":\"date_trunc\",\"source_column\":\"event_time\",\"unit\":\"DAY\"}}],"
                        + "\"bucket_count\":32,\"properties\":{},\"custom_properties\":{},"
                        + "\"created_time\":1735538268,\"modified_time\":1735538270}";
        TableRegistration unresolvedRegistration =
                JsonSerdeUtils.readValue(
                        withoutTimeZone.getBytes(StandardCharsets.UTF_8),
                        TableRegistrationJsonSerde.INSTANCE);
        Schema ltzSchema =
                Schema.newBuilder()
                        .column("event_time", DataTypes.TIMESTAMP_LTZ().copy(false))
                        .build();

        assertThatThrownBy(
                        () ->
                                unresolvedRegistration.toTableInfo(
                                        TablePath.of("db", "ltz_without_zone"),
                                        new SchemaInfo(ltzSchema, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must contain a resolved time zone");

        String withTimeZone =
                withoutTimeZone.replace(
                        "\"unit\":\"DAY\"", "\"unit\":\"DAY\",\"time_zone\":\"UTC\"");
        TableRegistration resolvedRegistration =
                JsonSerdeUtils.readValue(
                        withTimeZone.getBytes(StandardCharsets.UTF_8),
                        TableRegistrationJsonSerde.INSTANCE);
        TableInfo tableInfo =
                resolvedRegistration.toTableInfo(
                        TablePath.of("db", "ltz_with_zone"), new SchemaInfo(ltzSchema, 1));
        DateTruncPartitionTransform transform =
                (DateTruncPartitionTransform)
                        tableInfo.getPartitionExpressions().get(0).getTransform();
        assertThat(transform.getTimeZone()).hasValue(ZoneId.of("UTC"));
    }

    @Test
    void testToTableInfoPreservesPartitionExpressions() {
        TableRegistration registration = createObjects()[2];
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                        .primaryKey("id", "event_time")
                        .build();

        TableInfo tableInfo =
                registration.toTableInfo(TablePath.of("db", "t"), new SchemaInfo(schema, 3));

        assertThat(tableInfo.getPartitionExpressions())
                .isEqualTo(registration.partitionExpressions);
        assertThat(tableInfo.toTableDescriptor().getPartitionExpressions())
                .isEqualTo(registration.partitionExpressions);
        assertThat(registration.toString()).contains("partitionExpressions");
    }

    @Override
    protected TableRegistration[] createObjects() {
        TableRegistration[] tableRegistrations = new TableRegistration[3];

        tableRegistrations[0] =
                new TableRegistration(
                        1234L,
                        "first-table",
                        Arrays.asList("a", "b"),
                        new TableDistribution(16, Arrays.asList("b", "c")),
                        Maps.newHashMap(),
                        Collections.singletonMap("custom-3", "\"300\""),
                        "file://local/remote",
                        1735538268L,
                        1735538270L);

        tableRegistrations[1] =
                new TableRegistration(
                        1234L,
                        "second-table",
                        Collections.emptyList(),
                        new TableDistribution(32, Collections.emptyList()),
                        Collections.singletonMap("option-3", "300"),
                        Maps.newHashMap(),
                        null,
                        -1,
                        -1);

        tableRegistrations[2] =
                new TableRegistration(
                        1234L,
                        "implicit-partition-table",
                        Collections.singletonList("event_day"),
                        Collections.singletonList(
                                PartitionExpression.of(
                                        "event_day",
                                        DateTruncPartitionTransform.of(
                                                "event_time", AutoPartitionTimeUnit.DAY))),
                        new TableDistribution(32, Collections.singletonList("id")),
                        Maps.newHashMap(),
                        Maps.newHashMap(),
                        null,
                        1735538268L,
                        1735538270L);

        return tableRegistrations;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"table_id\":1234,\"comment\":\"first-table\",\"partition_key\":[\"a\",\"b\"],"
                    + "\"bucket_key\":[\"b\",\"c\"],\"bucket_count\":16,\"properties\":{},\"custom_properties\":{\"custom-3\":\"\\\"300\\\"\"},\"remote_data_dir\":\"file://local/remote\",\"created_time\":1735538268,\"modified_time\":1735538270}",
            "{\"version\":1,\"table_id\":1234,\"comment\":\"second-table\",\"bucket_count\":32,\"properties\":{\"option-3\":\"300\"},\"custom_properties\":{},\"created_time\":-1,\"modified_time\":-1}",
            "{\"version\":1,\"table_id\":1234,\"comment\":\"implicit-partition-table\",\"partition_key\":[\"event_day\"],\"partition_expressions\":[{\"virtual_partition_spec_key\":\"event_day\",\"transform\":{\"type\":\"date_trunc\",\"source_column\":\"event_time\",\"unit\":\"DAY\"}}],\"bucket_key\":[\"id\"],\"bucket_count\":32,\"properties\":{},\"custom_properties\":{},\"created_time\":1735538268,\"modified_time\":1735538270}",
        };
    }

    private static TableRegistration tableRegistration(
            List<String> partitionKeys, List<PartitionExpression> partitionExpressions) {
        return new TableRegistration(
                1234L,
                "implicit-partition-table",
                partitionKeys,
                partitionExpressions,
                new TableDistribution(32, Collections.singletonList("id")),
                Maps.newHashMap(),
                Maps.newHashMap(),
                null,
                1735538268L,
                1735538270L);
    }
}
