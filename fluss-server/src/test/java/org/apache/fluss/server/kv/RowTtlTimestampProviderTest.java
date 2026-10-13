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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowTtlTimestampProvider}. */
class RowTtlTimestampProviderTest {

    private static final short SCHEMA_ID = 0;
    private static final ZoneId UTC = ZoneId.of("UTC");

    @Test
    void testProcessTimeProviderUsesPreparedBatchTimestamp() {
        RowTtlTimestampProvider provider =
                RowTtlTimestampProvider.create(
                        processTimeTableConfig(), schemaGetter(schema()), UTC);

        provider.prepareForBatch(1234L);

        assertThat(provider.applyAsLong(row(1, 100L))).isEqualTo(1234L);

        provider.prepareForBatch(5678L);
        assertThat(provider.applyAsLong(row(2, 200L))).isEqualTo(5678L);
    }

    @Test
    void testBigintEventTimeProviderReadsEpochMillis() {
        RowTtlTimestampProvider provider =
                RowTtlTimestampProvider.create(eventTimeTableConfig(), schemaGetter(schema()), UTC);

        provider.prepareForBatch(999L);

        assertThat(provider.applyAsLong(row(1, 1234L))).isEqualTo(1234L);
    }

    @Test
    void testTimestampLtzEventTimeProviderReadsEpochMillis() {
        Schema timestampLtzSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("event_time", DataTypes.TIMESTAMP_LTZ(3))
                        .primaryKey("id")
                        .build();
        RowTtlTimestampProvider provider =
                RowTtlTimestampProvider.create(
                        eventTimeTableConfig(), schemaGetter(timestampLtzSchema), UTC);

        BinaryRow row =
                compactedRow(
                        timestampLtzSchema.getRowType(),
                        new Object[] {1, TimestampLtz.fromEpochMillis(5678L)});

        assertThat(provider.applyAsLong(row)).isEqualTo(5678L);
    }

    @Test
    void testTimestampEventTimeProviderUsesServerTimeZone() {
        ZoneId serverTimeZone = ZoneId.of("Asia/Shanghai");
        LocalDateTime eventTime = LocalDateTime.of(2026, 8, 25, 10, 0);
        Schema timestampSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("event_time", DataTypes.TIMESTAMP(3))
                        .primaryKey("id")
                        .build();
        RowTtlTimestampProvider provider =
                RowTtlTimestampProvider.create(
                        eventTimeTableConfig(), schemaGetter(timestampSchema), serverTimeZone);

        BinaryRow row =
                compactedRow(
                        timestampSchema.getRowType(),
                        new Object[] {1, TimestampNtz.fromLocalDateTime(eventTime)});

        assertThat(provider.applyAsLong(row))
                .isEqualTo(eventTime.atZone(serverTimeZone).toInstant().toEpochMilli());
    }

    @Test
    void testNullEventTimeNeverExpires() {
        RowTtlTimestampProvider provider =
                RowTtlTimestampProvider.create(eventTimeTableConfig(), schemaGetter(schema()), UTC);

        assertThat(provider.applyAsLong(row(1, null)))
                .isEqualTo(RowTtlTimestampProvider.NEVER_EXPIRE_TIMESTAMP_MS);
    }

    private static TableConfig processTimeTableConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.TABLE_KV_TTL, java.time.Duration.ofHours(1));
        return new TableConfig(configuration);
    }

    private static TableConfig eventTimeTableConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.TABLE_KV_TTL, java.time.Duration.ofHours(1));
        configuration.setString(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN, "event_time");
        return new TableConfig(configuration);
    }

    private static TestingSchemaGetter schemaGetter(Schema schema) {
        return new TestingSchemaGetter(new SchemaInfo(schema, SCHEMA_ID));
    }

    private static Schema schema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("event_time", DataTypes.BIGINT())
                .primaryKey("id")
                .build();
    }

    private static BinaryRow row(int id, Long eventTime) {
        return compactedRow(schema().getRowType(), new Object[] {id, eventTime});
    }
}
