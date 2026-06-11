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

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionKey;
import org.apache.fluss.metadata.PartitionTransform;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TransformType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TableDescriptorValidation}. */
class TableDescriptorValidationTest {

    @Test
    void testImplicitPartitionAllowsAutoPartition() {
        TableDescriptor descriptor =
                implicitPartitionDescriptor("event_day", AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .build();

        assertThatCode(() -> validate(descriptor)).doesNotThrowAnyException();
    }

    @Test
    void testRejectsPreCreateForMultipleImplicitPartitionKeys() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("region", DataTypes.STRING().copy(false))
                                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                        .build())
                        .partitionedByKeys(
                                PartitionKey.column("region"),
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_day",
                                                DateTruncPartitionTransform.of(
                                                        "event_time", AutoPartitionTimeUnit.DAY))))
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "event_day")
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 1)
                        .build();

        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("multiple partition keys")
                .hasMessageContaining("must be set to 0");
    }

    @Test
    void testImplicitPartitionDatalakeValidationIsDelegatedToLakeCatalog() {
        TableDescriptor descriptor =
                implicitPartitionDescriptor("event_day", AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();

        assertThatCode(() -> validate(descriptor)).doesNotThrowAnyException();
    }

    @Test
    void testImplicitPartitionAllowsAlterDatalakeEnabled() {
        TableDescriptor descriptor =
                implicitPartitionDescriptor("event_day", AutoPartitionTimeUnit.DAY).build();
        TableInfo tableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "test_table"), 1, 1, descriptor, "/tmp/test", 1, 1);

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateAlterTableProperties(
                                        tableInfo,
                                        Collections.singleton(
                                                ConfigOptions.TABLE_DATALAKE_ENABLED.key())))
                .doesNotThrowAnyException();
    }

    @Test
    void testDateTruncRejectsDateHour() {
        TableDescriptor descriptor =
                dateTruncDescriptor(
                        "event_date",
                        DataTypes.DATE().copy(false),
                        "event_hour",
                        AutoPartitionTimeUnit.HOUR);

        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidTableException.class)
                .hasMessage("DATE_TRUNC partition transform does not support DATE + HOUR.");
    }

    @Test
    void testDateTruncAcceptsSupportedSourceTypes() {
        assertThatCode(
                        () ->
                                validate(
                                        dateTruncDescriptor(
                                                "event_date",
                                                DataTypes.DATE().copy(false),
                                                "event_day",
                                                AutoPartitionTimeUnit.DAY)))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                validate(
                                        dateTruncDescriptor(
                                                "event_time",
                                                DataTypes.TIMESTAMP().copy(false),
                                                "event_hour",
                                                AutoPartitionTimeUnit.HOUR)))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                validate(
                                        dateTruncDescriptor(
                                                        "event_ltz",
                                                        DataTypes.TIMESTAMP_LTZ().copy(false),
                                                        "event_hour",
                                                        AutoPartitionTimeUnit.HOUR)
                                                .withResolvedPartitionExpressionTimeZone(
                                                        ZoneId.of("UTC"))))
                .doesNotThrowAnyException();
    }

    @Test
    void testDateTruncRejectsUnsupportedSourceType() {
        TableDescriptor descriptor =
                dateTruncDescriptor(
                        "event_time",
                        DataTypes.STRING().copy(false),
                        "event_day",
                        AutoPartitionTimeUnit.DAY);

        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "DATE_TRUNC partition transform does not support source column 'event_time'");
    }

    @Test
    void testRejectsUnsupportedTransformImplementation() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                        .build())
                        .partitionedByKeys(
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_day", new UnsupportedPartitionTransform())))
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .build();

        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidTableException.class)
                .hasMessage("Unsupported partition transform type: DATE_TRUNC");
    }

    @Test
    void testVirtualPartitionKeyDoesNotNeedPhysicalSchemaColumn() {
        TableDescriptor descriptor =
                implicitPartitionDescriptor("event_day", AutoPartitionTimeUnit.DAY).build();

        assertThatCode(() -> validate(descriptor)).doesNotThrowAnyException();
    }

    @Test
    void testRejectsInvalidVirtualPartitionKeyName() {
        assertThatThrownBy(
                        () ->
                                validate(
                                        implicitPartitionDescriptor("", AutoPartitionTimeUnit.DAY)
                                                .build()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Virtual partition spec key '' is invalid");

        assertThatThrownBy(
                        () ->
                                validate(
                                        implicitPartitionDescriptor(
                                                        "__event_day", AutoPartitionTimeUnit.DAY)
                                                .build()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Virtual partition spec key '__event_day' is invalid");

        assertThatThrownBy(
                        () ->
                                validate(
                                        implicitPartitionDescriptor(
                                                        "event$day", AutoPartitionTimeUnit.DAY)
                                                .build()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Virtual partition spec key 'event$day' is invalid");
    }

    @Test
    void testRejectsSystemColumnVirtualPartitionKey() {
        for (String systemColumn :
                Arrays.asList(
                        TableDescriptor.OFFSET_COLUMN_NAME,
                        TableDescriptor.TIMESTAMP_COLUMN_NAME,
                        TableDescriptor.BUCKET_COLUMN_NAME,
                        TableDescriptor.CHANGE_TYPE_COLUMN,
                        TableDescriptor.LOG_OFFSET_COLUMN,
                        TableDescriptor.COMMIT_TIMESTAMP_COLUMN)) {
            assertThatThrownBy(
                            () ->
                                    validate(
                                            implicitPartitionDescriptor(
                                                            systemColumn, AutoPartitionTimeUnit.DAY)
                                                    .build()))
                    .isInstanceOf(InvalidTableException.class)
                    .hasMessage(
                            "Virtual partition spec key '"
                                    + systemColumn
                                    + "' is reserved for system columns.");
        }
    }

    private static TableDescriptor.Builder implicitPartitionDescriptor(
            String virtualPartitionKey, AutoPartitionTimeUnit timeUnit) {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                .build())
                .partitionedByKeys(
                        PartitionKey.expression(
                                PartitionExpression.of(
                                        virtualPartitionKey,
                                        DateTruncPartitionTransform.of("event_time", timeUnit))))
                .distributedBy(1)
                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1);
    }

    private static TableDescriptor dateTruncDescriptor(
            String sourceColumn,
            DataType sourceType,
            String virtualPartitionKey,
            AutoPartitionTimeUnit timeUnit) {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column(sourceColumn, sourceType).build())
                .partitionedByKeys(
                        PartitionKey.expression(
                                PartitionExpression.of(
                                        virtualPartitionKey,
                                        DateTruncPartitionTransform.of(sourceColumn, timeUnit))))
                .distributedBy(1)
                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                .build();
    }

    private static void validate(TableDescriptor descriptor) {
        TableDescriptorValidation.validateTableDescriptor(descriptor, 10, null);
    }

    private static class UnsupportedPartitionTransform implements PartitionTransform {
        private static final long serialVersionUID = 1L;

        @Override
        public TransformType getType() {
            return TransformType.DATE_TRUNC;
        }

        @Override
        public List<String> getSourceColumns() {
            return Collections.singletonList("event_time");
        }
    }
}
