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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionKey;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for create-time defaults applied by {@link CoordinatorService}. */
class CoordinatorServiceTest {

    @Test
    void testImplicitPartitionDefaultsForTimestampNtz() {
        TableDescriptor resolved =
                CoordinatorService.applyImplicitPartitionDefaults(
                        implicitDescriptor(DataTypes.TIMESTAMP().copy(false), true).build());

        Configuration properties = Configuration.fromMap(resolved.getProperties());
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED)).isTrue();
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION)).isEqualTo(7);
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_KEY)).isEqualTo("event_day");
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT))
                .isEqualTo(AutoPartitionTimeUnit.DAY);
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE)).isEqualTo("UTC");
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE)).isZero();
        assertThat(
                        ((DateTruncPartitionTransform)
                                        resolved.getPartitionExpressions().get(0).getTransform())
                                .getTimeZone())
                .isEmpty();
    }

    @Test
    void testImplicitPartitionDefaultsForTimestampLtz() {
        TableDescriptor resolved =
                CoordinatorService.applyImplicitPartitionDefaults(
                        implicitDescriptor(DataTypes.TIMESTAMP_LTZ().copy(false), false)
                                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "Etc/UTC")
                                .build());

        Configuration properties = Configuration.fromMap(resolved.getProperties());
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE)).isEqualTo("UTC");
        assertThat(
                        ((DateTruncPartitionTransform)
                                        resolved.getPartitionExpressions().get(0).getTransform())
                                .getTimeZone())
                .hasValue(ZoneId.of("UTC"));
    }

    @Test
    void testRejectInvalidImplicitPartitionLifecycleOptions() {
        assertThatThrownBy(
                        () ->
                                CoordinatorService.applyImplicitPartitionDefaults(
                                        implicitDescriptor(DataTypes.TIMESTAMP().copy(false), false)
                                                .property(
                                                        ConfigOptions.TABLE_AUTO_PARTITION_ENABLED,
                                                        false)
                                                .build()))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("require 'table.auto-partition.enabled'=true");

        assertThatThrownBy(
                        () ->
                                CoordinatorService.applyImplicitPartitionDefaults(
                                        implicitDescriptor(DataTypes.TIMESTAMP().copy(false), false)
                                                .property(
                                                        ConfigOptions
                                                                .TABLE_AUTO_PARTITION_NUM_RETENTION,
                                                        -1)
                                                .build()))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("require non-negative");

        assertThatThrownBy(
                        () ->
                                CoordinatorService.applyImplicitPartitionDefaults(
                                        implicitDescriptor(DataTypes.TIMESTAMP().copy(false), false)
                                                .property(
                                                        ConfigOptions
                                                                .TABLE_AUTO_PARTITION_TIME_UNIT,
                                                        AutoPartitionTimeUnit.HOUR)
                                                .build()))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("must match implicit partition transform unit DAY");

        assertThatThrownBy(
                        () ->
                                CoordinatorService.applyImplicitPartitionDefaults(
                                        implicitDescriptor(
                                                        DataTypes.TIMESTAMP_LTZ().copy(false),
                                                        false)
                                                .property(
                                                        ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE,
                                                        "Asia/Shanghai")
                                                .build()))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("must match implicit partition transform time zone UTC");
    }

    @Test
    void testExplicitImplicitPartitionLifecycleOptionsArePreserved() {
        TableDescriptor resolved =
                CoordinatorService.applyImplicitPartitionDefaults(
                        implicitDescriptor(DataTypes.TIMESTAMP().copy(false), false)
                                .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                                .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "event_day")
                                .property(
                                        ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                        AutoPartitionTimeUnit.DAY)
                                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "GMT")
                                .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 30)
                                .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 2)
                                .build());

        Configuration properties = Configuration.fromMap(resolved.getProperties());
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED)).isTrue();
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_KEY)).isEqualTo("event_day");
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT))
                .isEqualTo(AutoPartitionTimeUnit.DAY);
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE)).isEqualTo("GMT");
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION)).isEqualTo(30);
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE)).isEqualTo(2);
    }

    @Test
    void testMultipleImplicitPartitionKeysRequireExplicitLifecycleKey() {
        TableDescriptor descriptor = multipleImplicitDescriptor().build();

        assertThatThrownBy(() -> CoordinatorService.applyImplicitPartitionDefaults(descriptor))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("multiple temporal virtual keys")
                .hasMessageContaining(ConfigOptions.TABLE_AUTO_PARTITION_KEY.key());

        TableDescriptor resolved =
                CoordinatorService.applyImplicitPartitionDefaults(
                        multipleImplicitDescriptor()
                                .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "created_month")
                                .build());
        Configuration properties = Configuration.fromMap(resolved.getProperties());
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_KEY))
                .isEqualTo("created_month");
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT))
                .isEqualTo(AutoPartitionTimeUnit.MONTH);
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE)).isZero();

        assertThatThrownBy(
                        () ->
                                CoordinatorService.applyImplicitPartitionDefaults(
                                        multipleImplicitDescriptor()
                                                .property(
                                                        ConfigOptions.TABLE_AUTO_PARTITION_KEY,
                                                        "created_month")
                                                .property(
                                                        ConfigOptions
                                                                .TABLE_AUTO_PARTITION_NUM_PRECREATE,
                                                        1)
                                                .build()))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Implicit partition tables with multiple partition keys require")
                .hasMessageContaining(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key());
    }

    @Test
    void testImplicitLifecycleKeyMustBeVirtualDateTruncKey() {
        assertThatThrownBy(
                        () ->
                                CoordinatorService.applyImplicitPartitionDefaults(
                                        implicitDescriptor(DataTypes.TIMESTAMP().copy(false), true)
                                                .property(
                                                        ConfigOptions.TABLE_AUTO_PARTITION_KEY,
                                                        "region")
                                                .build()))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("must identify a virtual DATE_TRUNC partition key");
    }

    @Test
    void testDateImplicitPartitionUsesUtcLifecycleWithoutTransformTimeZone() {
        TableDescriptor resolved =
                CoordinatorService.applyImplicitPartitionDefaults(
                        implicitDescriptor(DataTypes.DATE().copy(false), false).build());

        Configuration properties = Configuration.fromMap(resolved.getProperties());
        assertThat(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE)).isEqualTo("UTC");
        DateTruncPartitionTransform transform =
                (DateTruncPartitionTransform)
                        resolved.getPartitionExpressions().get(0).getTransform();
        assertThat(transform.getTimeZone()).isEmpty();
    }

    private static TableDescriptor.Builder implicitDescriptor(
            DataType timestampType, boolean mixedPartitionKeys) {
        Schema schema =
                Schema.newBuilder()
                        .column("region", DataTypes.STRING().copy(false))
                        .column("event_time", timestampType)
                        .build();
        TableDescriptor.Builder builder = TableDescriptor.builder().schema(schema);
        if (mixedPartitionKeys) {
            builder.partitionedByKeys(
                    PartitionKey.column("region"),
                    PartitionKey.expression(
                            PartitionExpression.of(
                                    "event_day",
                                    DateTruncPartitionTransform.of(
                                            "event_time", AutoPartitionTimeUnit.DAY))));
        } else {
            builder.partitionedByKeys(
                    PartitionKey.expression(
                            PartitionExpression.of(
                                    "event_day",
                                    DateTruncPartitionTransform.of(
                                            "event_time", AutoPartitionTimeUnit.DAY))));
        }
        return builder.distributedBy(1);
    }

    private static TableDescriptor.Builder multipleImplicitDescriptor() {
        Schema schema =
                Schema.newBuilder()
                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                        .column("created_at", DataTypes.TIMESTAMP().copy(false))
                        .build();
        return TableDescriptor.builder()
                .schema(schema)
                .partitionedByKeys(
                        PartitionKey.expression(
                                PartitionExpression.of(
                                        "event_day",
                                        DateTruncPartitionTransform.of(
                                                "event_time", AutoPartitionTimeUnit.DAY))),
                        PartitionKey.expression(
                                PartitionExpression.of(
                                        "created_month",
                                        DateTruncPartitionTransform.of(
                                                "created_at", AutoPartitionTimeUnit.MONTH))))
                .distributedBy(1);
    }
}
