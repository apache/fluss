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

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.metadata.TableDescriptor}. */
class TableDescriptorTest {

    private static final Schema SCHEMA_1 =
            Schema.newBuilder()
                    .column("f0", DataTypes.STRING())
                    .column("f1", DataTypes.BIGINT())
                    .column("f3", DataTypes.STRING())
                    .primaryKey("f0", "f3")
                    .build();

    private static final ConfigOption<Boolean> OPTION_A =
            ConfigBuilder.key("a").booleanType().noDefaultValue();

    private static final ConfigOption<Integer> OPTION_B =
            ConfigBuilder.key("b").intType().noDefaultValue();

    @Test
    void testBasic() {
        final TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(SCHEMA_1)
                        .partitionedBy("f0")
                        .comment("Test Comment")
                        .build();

        assertThat(descriptor.getSchema()).isEqualTo(SCHEMA_1);
        assertThat(descriptor.isPartitioned()).isTrue();
        assertThat(descriptor.getPartitionKeys()).hasSize(1);
        assertThat(descriptor.getPartitionKeys().get(0)).isEqualTo("f0");
        assertThat(descriptor.getProperties()).hasSize(0);
        assertThat(descriptor.getComment().orElse(null)).isEqualTo("Test Comment");

        Optional<TableDescriptor.TableDistribution> distribution =
                descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).isEmpty();
        assertThat(distribution.get().getBucketKeys()).hasSize(1);
        assertThat(distribution.get().getBucketKeys().get(0)).isEqualTo("f3");
    }

    @Test
    void testProperties() {
        final TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().build())
                        .property(OPTION_A, false)
                        .property(OPTION_B, 42)
                        .property("c", "C")
                        .customProperty("d", "D")
                        .build();

        assertThat(descriptor.getProperties()).hasSize(3);
        assertThat(descriptor.getProperties().get("a")).isEqualTo("false");
        assertThat(descriptor.getProperties().get("b")).isEqualTo("42");
        assertThat(descriptor.getProperties().get("c")).isEqualTo("C");
        assertThat(descriptor.getProperties().get("d")).isNull();
        assertThat(descriptor.getCustomProperties().get("d")).isEqualTo("D");
    }

    @Test
    void testDistribution() {
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(SCHEMA_1).distributedBy(10, "f0", "f3").build();
        Optional<TableDescriptor.TableDistribution> distribution =
                descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).hasValue(10);
        assertThat(distribution.get().getBucketKeys()).hasSize(2);
        assertThat(distribution.get().getBucketKeys().get(0)).isEqualTo("f0", "f3");
        assertThat(descriptor.isDefaultBucketKey()).isTrue();

        // a subset of primary key
        descriptor = TableDescriptor.builder().schema(SCHEMA_1).distributedBy(10, "f3").build();
        distribution = descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).hasValue(10);
        assertThat(distribution.get().getBucketKeys()).isEqualTo(Collections.singletonList("f3"));
        assertThat(descriptor.isDefaultBucketKey()).isFalse();

        // default bucket key for partitioned table
        descriptor = TableDescriptor.builder().schema(SCHEMA_1).partitionedBy("f0").build();
        distribution = descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).isEmpty();
        assertThat(distribution.get().getBucketKeys()).isEqualTo(Collections.singletonList("f3"));
        assertThat(descriptor.isDefaultBucketKey()).isTrue();

        // test subset of primary key for partitioned table
        descriptor =
                TableDescriptor.builder()
                        .schema(SCHEMA_1)
                        .partitionedBy("f0")
                        .distributedBy(10, "f3")
                        .build();
        distribution = descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).hasValue(10);
        assertThat(distribution.get().getBucketKeys()).isEqualTo(Collections.singletonList("f3"));
        assertThat(descriptor.isDefaultBucketKey()).isTrue();
    }

    @Test
    void testSchemaWithoutPrimaryKeyAndDistributionWithEmptyBucketKeys() {
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .build();
        final TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(12).build();
        Optional<TableDescriptor.TableDistribution> distribution =
                descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).hasValue(12);
        assertThat(distribution.get().getBucketKeys()).hasSize(0);
        assertThat(descriptor.isDefaultBucketKey()).isTrue();
    }

    @Test
    void testPrimaryKeyDifferentWithBucketKeys() {
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(SCHEMA_1)
                                        .distributedBy(12, "f1")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Bucket keys must be a subset of primary keys excluding partition keys for primary-key tables. "
                                + "The primary keys are [f0, f3], the partition keys are [], "
                                + "but the user-defined bucket keys are [f1].");

        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(SCHEMA_1)
                                        .distributedBy(12, "f0", "f1")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Bucket keys must be a subset of primary keys excluding partition keys for primary-key tables. "
                                + "The primary keys are [f0, f3], the partition keys are [], "
                                + "but the user-defined bucket keys are [f0, f1].");

        // bucket key shouldn't include partition key
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(SCHEMA_1)
                                        .partitionedBy("f0")
                                        .distributedBy(3, "f0", "f3")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Bucket key [f0, f3] shouldn't include any column in partition keys [f0].");
    }

    @Test
    void testSchemaWithPrimaryKeysButDistributionIsNull() {
        // primary key is "f0", but distribution is null, we will use primary key as bucket key.
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(SCHEMA_1)
                        .partitionedBy("f0")
                        .comment("Test Comment")
                        .build();
        Optional<TableDescriptor.TableDistribution> distribution =
                descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).isEmpty();
        assertThat(distribution.get().getBucketKeys()).hasSize(1);
        assertThat(distribution.get().getBucketKeys().get(0)).isEqualTo("f3");
    }

    @Test
    void testSchemaWithPrimaryKeysButDistributionWithEmptyBucketKey() {
        // primary key is "f0", but bucket key is empty, we will use primary key as bucket key.
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(SCHEMA_1)
                        .partitionedBy("f0")
                        .distributedBy(10)
                        .comment("Test Comment")
                        .build();
        Optional<TableDescriptor.TableDistribution> distribution =
                descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).hasValue(10);
        assertThat(distribution.get().getBucketKeys()).hasSize(1);
        assertThat(distribution.get().getBucketKeys().get(0)).isEqualTo("f3");
    }

    @Test
    void testWithProperties() {
        final TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(SCHEMA_1)
                        .partitionedBy("f0")
                        .comment("Test Comment")
                        .property(OPTION_A, true)
                        .build();

        final TableDescriptor copy = descriptor.withProperties(new HashMap<>());
        assertThat(copy.isPartitioned()).isEqualTo(descriptor.isPartitioned());
        assertThat(copy.getTableDistribution()).isEqualTo(descriptor.getTableDistribution());
        assertThat(copy.getComment()).isEqualTo(descriptor.getComment());
        assertThat(copy.getPartitionKeys()).isEqualTo(descriptor.getPartitionKeys());
        assertThat(copy.getSchema()).isEqualTo(descriptor.getSchema());
        assertThat(copy.getProperties()).hasSize(0);
    }

    @Test
    void testImplicitPartitionDescriptorCopyHelpersPreserveExpressions() {
        TableDescriptor descriptor = implicitPartitionDescriptor().build();

        TableDescriptor fromExistingBuilder = TableDescriptor.builder(descriptor).build();
        assertThat(fromExistingBuilder).isEqualTo(descriptor);
        assertThat(fromExistingBuilder.getPartitionExpressions())
                .isEqualTo(descriptor.getPartitionExpressions());

        Map<String, String> properties = Collections.singletonMap("table.test.option", "1");
        assertThat(descriptor.withProperties(properties).getPartitionExpressions())
                .isEqualTo(descriptor.getPartitionExpressions());
        assertThat(descriptor.withBucketCount(8).getPartitionExpressions())
                .isEqualTo(descriptor.getPartitionExpressions());
        assertThat(descriptor.withDataLakeFormat(DataLakeFormat.PAIMON).getPartitionExpressions())
                .isEqualTo(descriptor.getPartitionExpressions());
    }

    @Test
    void testExistingDescriptorBuilderKeepsPartitionDeclarationMode() {
        TableDescriptor implicitDescriptor = implicitPartitionDescriptor().build();

        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder(implicitDescriptor)
                                        .partitionedBy("region")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "partitionedBy(...) and partitionedByKeys(...) cannot be mixed in the same builder.");

        TableDescriptor explicitDescriptor =
                TableDescriptor.builder().schema(SCHEMA_1).partitionedBy("f0").build();
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder(explicitDescriptor)
                                        .partitionedByKeys(PartitionKey.column("f0"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "partitionedBy(...) and partitionedByKeys(...) cannot be mixed in the same builder.");
    }

    @Test
    void testRepeatedPartitionDeclarationClearsStaleStateWithinSameMode() {
        TableDescriptor legacyDescriptor =
                TableDescriptor.builder()
                        .schema(SCHEMA_1)
                        .partitionedBy("f0")
                        .partitionedBy("f3")
                        .build();
        assertThat(legacyDescriptor.getPartitionKeys()).containsExactly("f3");
        assertThat(legacyDescriptor.getPartitionExpressions()).isEmpty();

        TableDescriptor partitionKeysDescriptor =
                implicitPartitionDescriptor()
                        .partitionedByKeys(PartitionKey.column("region"))
                        .build();
        assertThat(partitionKeysDescriptor.getPartitionKeys()).containsExactly("region");
        assertThat(partitionKeysDescriptor.getPartitionExpressions()).isEmpty();
    }

    @Test
    void testTableInfoToDescriptorPreservesImplicitPartitionExpressions() {
        TableDescriptor descriptor = implicitPartitionDescriptor().build();
        TableInfo tableInfo =
                TableInfo.of(TablePath.of("db", "t"), 1L, 1, descriptor, null, 1L, 1L);

        TableDescriptor roundTrippedDescriptor = tableInfo.toTableDescriptor();

        assertThat(roundTrippedDescriptor).isEqualTo(descriptor);
        assertThat(roundTrippedDescriptor.getPhysicalPartitionKeys())
                .isEqualTo(tableInfo.getPhysicalPartitionKeys());
        assertThat(roundTrippedDescriptor.getVirtualPartitionKeys())
                .isEqualTo(tableInfo.getVirtualPartitionKeys());
        assertThat(roundTrippedDescriptor.getPartitionSourceColumns())
                .isEqualTo(tableInfo.getPartitionSourceColumns());
        assertThat(roundTrippedDescriptor.getPartitionInputColumns())
                .isEqualTo(tableInfo.getPartitionInputColumns());
    }

    @Test
    void testImplicitPartitionMetadataObjectMethodsIncludeExpressions() {
        TableDescriptor descriptor = implicitPartitionDescriptor().build();
        TableDescriptor sameDescriptor = TableDescriptor.builder(descriptor).build();
        TableDescriptor physicalOnlyDescriptor =
                implicitPartitionDescriptor()
                        .partitionedByKeys(PartitionKey.column("region"))
                        .build();

        assertThat(descriptor).isEqualTo(sameDescriptor);
        assertThat(descriptor.hashCode()).isEqualTo(sameDescriptor.hashCode());
        assertThat(descriptor).isNotEqualTo(physicalOnlyDescriptor);
        assertThat(descriptor.toString()).contains("partitionExpressions");

        TableInfo tableInfo =
                TableInfo.of(
                        TablePath.of("db", "t"),
                        1L,
                        1,
                        descriptor.withBucketCount(1),
                        null,
                        1L,
                        1L);
        TableInfo physicalOnlyTableInfo =
                TableInfo.of(TablePath.of("db", "t"), 1L, 1, physicalOnlyDescriptor, null, 1L, 1L);

        assertThat(tableInfo).isNotEqualTo(physicalOnlyTableInfo);
        assertThat(tableInfo.toString()).contains("partitionExpressions");
    }

    @Test
    void testResolvedImplicitPartitionTimeZoneDoesNotUseAutoPartitionTimeZone() {
        TableDescriptor descriptor =
                implicitPartitionDescriptor()
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "UTC")
                        .build();

        TableDescriptor resolved =
                descriptor.withResolvedPartitionExpressionTimeZone(ZoneId.of("Asia/Shanghai"));
        DateTruncPartitionTransform transform =
                (DateTruncPartitionTransform)
                        resolved.getPartitionExpressions().get(0).getTransform();

        assertThat(transform.getTimeZone()).hasValue(ZoneId.of("Asia/Shanghai"));
    }

    @Test
    void testInvalidImplicitPartitionMetadata() {
        assertThatThrownBy(
                        () ->
                                implicitPartitionDescriptor()
                                        .partitionedByKeys(
                                                PartitionKey.expression(
                                                        PartitionExpression.of(
                                                                "region",
                                                                DateTruncPartitionTransform.of(
                                                                        "event_time",
                                                                        AutoPartitionTimeUnit
                                                                                .DAY))))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Virtual partition spec key 'region' conflicts with a physical column.");

        assertThatThrownBy(
                        () ->
                                implicitPartitionDescriptor()
                                        .partitionedByKeys(
                                                PartitionKey.expression(
                                                        PartitionExpression.of(
                                                                "event_day",
                                                                DateTruncPartitionTransform.of(
                                                                        "event_time",
                                                                        AutoPartitionTimeUnit
                                                                                .DAY))),
                                                PartitionKey.expression(
                                                        PartitionExpression.of(
                                                                "event_day",
                                                                DateTruncPartitionTransform.of(
                                                                        "event_time",
                                                                        AutoPartitionTimeUnit
                                                                                .MONTH))))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate partition keys are not allowed");
    }

    @Test
    void testOldServerIgnoringPartitionExpressionsFailsFast() {
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(
                                                Schema.newBuilder()
                                                        .column(
                                                                "event_time",
                                                                DataTypes.TIMESTAMP().copy(false))
                                                        .build())
                                        .partitionedBy("event_day")
                                        .distributedBy(1)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Partition key 'event_day' does not exist in the schema.");
    }

    @Test
    void testInvalidTableDescriptor() {
        // schema without primary key.
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .build();
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(schema)
                                        .partitionedBy("unknown_p")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Partition key 'unknown_p' does not exist in the schema.");

        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(schema)
                                        .distributedBy(12, "unknown_f")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bucket key 'unknown_f' does not exist in the schema.");

        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(
                                                Schema.newBuilder()
                                                        .column("id", DataTypes.INT())
                                                        .column("dt", DataTypes.STRING())
                                                        .column("a", DataTypes.BIGINT())
                                                        .column("ts", DataTypes.TIMESTAMP())
                                                        .primaryKey("id")
                                                        .build())
                                        .partitionedBy("dt")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Partitioned Primary Key Table requires physical partition keys [dt] is a subset of the primary key [id].");
    }

    @Test
    void testPartitionedTable() {
        // will partitioned keys are equal to primary keys, will no bucket key.
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(SCHEMA_1)
                                        .partitionedBy("f0", "f3")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Primary Key constraint [f0, f3] should not be same with partition fields [f0, f3].");

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(SCHEMA_1).partitionedBy("f0").build();
        assertThat(tableDescriptor.getTableDistribution().get().getBucketKeys())
                .isEqualTo(Collections.singletonList("f3"));

        // bucket key contains partitioned key, throw exception
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(SCHEMA_1)
                                        .partitionedBy("f0")
                                        .distributedBy(1, "f0", "f3")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Bucket key [f0, f3] shouldn't include any column in partition keys [f0].");
    }

    @Test
    void testPartitionedByKeysWithImplicitPartitionExpression() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("ts", DataTypes.TIMESTAMP().copy(false))
                        .column("region", DataTypes.STRING().copy(false))
                        .primaryKey("id", "ts", "region")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedByKeys(
                                PartitionKey.column("region"),
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                DateTruncPartitionTransform.of(
                                                        "ts", AutoPartitionTimeUnit.DAY))))
                        .build();

        assertThat(descriptor.getPartitionKeys()).containsExactly("region", "ts_day");
        assertThat(descriptor.getPhysicalPartitionKeys()).containsExactly("region");
        assertThat(descriptor.getVirtualPartitionKeys()).containsExactly("ts_day");
        assertThat(descriptor.getPartitionSourceColumns()).containsExactly("ts");
        assertThat(descriptor.getPartitionInputColumns()).containsExactly("region", "ts");
        assertThat(descriptor.getBucketKeys()).containsExactly("id", "ts");
        assertThat(descriptor.isDefaultBucketKey()).isTrue();
        assertThat(descriptor.getPartitionExpressions()).hasSize(1);

        TableInfo tableInfo =
                TableInfo.of(
                        TablePath.of("db", "t"),
                        1L,
                        1,
                        descriptor.withBucketCount(1),
                        null,
                        1L,
                        1L);
        assertThat(tableInfo.getPhysicalPrimaryKeys()).containsExactly("id", "ts");
        assertThat(tableInfo.getPartitionExpressions().get(0).getVirtualPartitionSpecKey())
                .hasValue("ts_day");
    }

    @Test
    void testPartitionedByAndPartitionedByKeysCannotBeMixed() {
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(SCHEMA_1)
                                        .partitionedBy("f0")
                                        .partitionedByKeys(PartitionKey.column("f3")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "partitionedBy(...) and partitionedByKeys(...) cannot be mixed in the same builder.");

        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(SCHEMA_1)
                                        .partitionedByKeys(PartitionKey.column("f0"))
                                        .partitionedBy("f3"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "partitionedBy(...) and partitionedByKeys(...) cannot be mixed in the same builder.");
    }

    @Test
    void testInvalidPartitionExpressionMetadata() {
        Schema nullableSourceSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("ts", DataTypes.TIMESTAMP())
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(nullableSourceSchema)
                                        .partitionedByKeys(
                                                PartitionKey.expression(
                                                        PartitionExpression.of(
                                                                DateTruncPartitionTransform.of(
                                                                        "ts",
                                                                        AutoPartitionTimeUnit
                                                                                .DAY))))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Partition transform source column 'ts' must be non-nullable.");

        Schema sourceNotInPkSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("ts", DataTypes.TIMESTAMP().copy(false))
                        .primaryKey("id")
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(sourceNotInPkSchema)
                                        .partitionedByKeys(
                                                PartitionKey.expression(
                                                        PartitionExpression.of(
                                                                DateTruncPartitionTransform.of(
                                                                        "ts",
                                                                        AutoPartitionTimeUnit
                                                                                .DAY))))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Partitioned Primary Key Table requires transform source column 'ts' is in the primary key [id].");
    }

    @Test
    void testTableInfoRejectsInconsistentPartitionExpressions() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                        .primaryKey("id", "event_time")
                        .build();

        assertThatThrownBy(
                        () ->
                                tableInfo(
                                        schema,
                                        Collections.singletonList("event_day"),
                                        Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Partition key 'event_day' does not exist in the schema or partition expressions.");

        assertThatThrownBy(
                        () ->
                                tableInfo(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT().copy(false))
                                                .column(
                                                        "event_day",
                                                        DataTypes.TIMESTAMP().copy(false))
                                                .primaryKey("id", "event_day")
                                                .build(),
                                        Collections.singletonList("event_day"),
                                        Collections.singletonList(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_day",
                                                                AutoPartitionTimeUnit.DAY)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Virtual partition spec key 'event_day' conflicts with a physical column.");

        assertThatThrownBy(
                        () ->
                                tableInfo(
                                        schema,
                                        Collections.singletonList("event_day"),
                                        Collections.singletonList(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "missing_time",
                                                                AutoPartitionTimeUnit.DAY)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Partition transform source column 'missing_time' does not exist in the schema.");

        assertThatThrownBy(
                        () ->
                                tableInfo(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT().copy(false))
                                                .column("event_time", DataTypes.TIMESTAMP())
                                                .build(),
                                        Collections.singletonList("event_day"),
                                        Collections.singletonList(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Partition transform source column 'event_time' must be non-nullable.");

        assertThatThrownBy(
                        () ->
                                tableInfo(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT().copy(false))
                                                .column("dt", DataTypes.STRING().copy(false))
                                                .primaryKey("id")
                                                .build(),
                                        Collections.singletonList("dt"),
                                        Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Partitioned Primary Key Table requires physical partition keys [dt] is a subset of the primary key [id].");

        assertThatThrownBy(
                        () ->
                                tableInfo(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT().copy(false))
                                                .column(
                                                        "event_time",
                                                        DataTypes.TIMESTAMP().copy(false))
                                                .primaryKey("id")
                                                .build(),
                                        Collections.singletonList("event_day"),
                                        Collections.singletonList(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Partitioned Primary Key Table requires transform source column 'event_time' is in the primary key [id].");

        assertThatThrownBy(
                        () ->
                                tableInfo(
                                        schema,
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
                                tableInfo(
                                        schema,
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

        assertThatThrownBy(
                        () ->
                                tableInfo(
                                        schema,
                                        Collections.singletonList("event_day"),
                                        Arrays.asList(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY)),
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.MONTH)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate virtual partition spec key 'event_day'.");
    }

    @Test
    void testInvalidListaggParameterEmptyDelimiter() {
        // LISTAGG with empty delimiter - should fail
        assertThatThrownBy(() -> AggFunctions.LISTAGG("").validateParameters())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a non-empty string");
    }

    @Test
    void testInvalidListaggParameterUnknownParameter() {
        // LISTAGG with unknown parameter - should fail
        Map<String, String> params = new HashMap<>();
        params.put("unknown_param", "value");

        assertThatThrownBy(
                        () -> AggFunctions.of(AggFunctionType.LISTAGG, params).validateParameters())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown_param")
                .hasMessageContaining("not supported");
    }

    @Test
    void testInvalidSumFunctionWithParameters() {
        // SUM function does not accept parameters - should fail
        Map<String, String> params = new HashMap<>();
        params.put("some_param", "value");

        assertThatThrownBy(() -> AggFunctions.of(AggFunctionType.SUM, params).validateParameters())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("some_param")
                .hasMessageContaining("not supported");
    }

    @Test
    void testValidateAggFunctionWithDataType() {
        Map<String, String> params = new HashMap<>();

        // invalid case
        assertThatThrownBy(
                        () ->
                                AggFunctions.of(AggFunctionType.BOOL_AND, params)
                                        .validateDataType(DataTypes.STRING()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column must be part of")
                .hasMessageContaining(
                        Arrays.deepToString(AggFunctionType.BOOL_AND.getSupportedDataTypeRoots()));

        assertThatThrownBy(
                        () ->
                                AggFunctions.of(AggFunctionType.SUM, params)
                                        .validateDataType(DataTypes.STRING()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column must be part of")
                .hasMessageContaining(
                        Arrays.deepToString(AggFunctionType.SUM.getSupportedDataTypeRoots()));

        assertThatThrownBy(
                        () ->
                                AggFunctions.of(AggFunctionType.MAX, params)
                                        .validateDataType(DataTypes.BOOLEAN()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column must be part of")
                .hasMessageContaining(
                        Arrays.deepToString(AggFunctionType.MAX.getSupportedDataTypeRoots()));

        // valid case
        AggFunctions.of(AggFunctionType.LAST_VALUE, params).validateDataType(DataTypes.STRING());
        AggFunctions.of(AggFunctionType.LISTAGG, params).validateDataType(DataTypes.STRING());
    }

    private static TableDescriptor.Builder implicitPartitionDescriptor() {
        Schema schema =
                Schema.newBuilder()
                        .column("region", DataTypes.STRING().copy(false))
                        .column("id", DataTypes.INT().copy(false))
                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                        .primaryKey("region", "id", "event_time")
                        .build();
        return TableDescriptor.builder()
                .schema(schema)
                .partitionedByKeys(
                        PartitionKey.column("region"),
                        PartitionKey.expression(
                                PartitionExpression.of(
                                        "event_day",
                                        DateTruncPartitionTransform.of(
                                                "event_time", AutoPartitionTimeUnit.DAY))))
                .distributedBy(4);
    }

    private static TableInfo tableInfo(
            Schema schema,
            List<String> partitionKeys,
            List<PartitionExpression> partitionExpressions) {
        return new TableInfo(
                TablePath.of("db", "t"),
                1L,
                0,
                schema,
                Collections.singletonList("id"),
                partitionKeys,
                partitionExpressions,
                1,
                new Configuration(),
                new Configuration(),
                null,
                null,
                1L,
                1L);
    }
}
