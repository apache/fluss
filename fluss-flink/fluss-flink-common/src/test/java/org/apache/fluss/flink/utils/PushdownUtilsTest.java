/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.flink.utils;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionKey;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PushdownUtils}. */
class PushdownUtilsTest {

    @Test
    void testComputeLookupPartitionSpecForImplicitPartition() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                        .primaryKey("id", "event_time")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedByKeys(
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_day",
                                                DateTruncPartitionTransform.of(
                                                        "event_time", AutoPartitionTimeUnit.DAY))))
                        .distributedBy(1)
                        .build();
        TableInfo tableInfo =
                TableInfo.of(TablePath.of("db", "table"), 1L, 1, descriptor, null, 1L, 1L);
        RowType lookupRowType = tableInfo.getRowType().project(schema.getPrimaryKeyIndexes());
        GenericRowData lookupRow =
                GenericRowData.of(
                        1, TimestampData.fromLocalDateTime(LocalDateTime.of(2026, 7, 29, 16, 30)));

        PartitionSpec partitionSpec =
                PushdownUtils.computeLookupPartitionSpec(
                        tableInfo, lookupRowType, new FlinkAsFlussRow(lookupRow));

        assertThat(partitionSpec.getSpecMap()).hasSize(1).containsEntry("event_day", "20260729");
    }

    @Test
    void testComputeLookupPartitionSpecForPhysicalAndImplicitPartitions() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("region", DataTypes.STRING().copy(false))
                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                        .primaryKey("id", "region", "event_time")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedByKeys(
                                PartitionKey.column("region"),
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_day",
                                                DateTruncPartitionTransform.of(
                                                        "event_time", AutoPartitionTimeUnit.DAY))))
                        .distributedBy(1)
                        .build();
        TableInfo tableInfo =
                TableInfo.of(TablePath.of("db", "table"), 1L, 1, descriptor, null, 1L, 1L);
        RowType lookupRowType = tableInfo.getRowType().project(schema.getPrimaryKeyIndexes());
        GenericRowData lookupRow =
                GenericRowData.of(
                        1,
                        StringData.fromString("us-east"),
                        TimestampData.fromLocalDateTime(LocalDateTime.of(2026, 7, 29, 16, 30)));

        PartitionSpec partitionSpec =
                PushdownUtils.computeLookupPartitionSpec(
                        tableInfo, lookupRowType, new FlinkAsFlussRow(lookupRow));

        assertThat(partitionSpec.getSpecMap())
                .hasSize(2)
                .containsEntry("region", "us-east")
                .containsEntry("event_day", "20260729");
    }

    @Test
    void testComputeLookupPartitionSpecForTimestampLtzUsesUtcBoundary() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("event_time", DataTypes.TIMESTAMP_LTZ().copy(false))
                        .primaryKey("id", "event_time")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedByKeys(
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_hour",
                                                DateTruncPartitionTransform.of(
                                                        "event_time", AutoPartitionTimeUnit.HOUR))))
                        .distributedBy(1)
                        .build()
                        .withResolvedPartitionExpressionTimeZone(ZoneId.of("UTC"));
        TableInfo tableInfo =
                TableInfo.of(TablePath.of("db", "table"), 1L, 1, descriptor, null, 1L, 1L);
        RowType lookupRowType = tableInfo.getRowType().project(schema.getPrimaryKeyIndexes());
        GenericRowData lookupRow =
                GenericRowData.of(
                        1, TimestampData.fromInstant(Instant.parse("2026-07-29T23:30:00Z")));

        PartitionSpec partitionSpec =
                PushdownUtils.computeLookupPartitionSpec(
                        tableInfo, lookupRowType, new FlinkAsFlussRow(lookupRow));

        assertThat(partitionSpec.getSpecMap()).hasSize(1).containsEntry("event_hour", "2026072923");
    }
}
