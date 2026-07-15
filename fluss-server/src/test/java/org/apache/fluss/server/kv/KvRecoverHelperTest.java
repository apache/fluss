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
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KvRecoverHelper}. */
class KvRecoverHelperTest {

    @Test
    void testRecoveredVersion3ValueUsesLogRecordTimestamp() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, DEFAULT_SCHEMA_ID));
        RowTtlTimestampProvider timestampProvider =
                RowTtlTimestampProvider.forRecovery(rowTtlProcessTimeConfig(), schemaGetter);
        ValueEncoder writeEncoder =
                ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_3, timestampProvider);

        byte[] recoveredValue =
                KvRecoverHelper.encodeRecoveredValue(
                        writeEncoder, timestampProvider, DEFAULT_SCHEMA_ID, row, 200L);
        BinaryValue decoded =
                new ValueDecoder(schemaGetter, KvFormat.COMPACTED, KV_FORMAT_VERSION_3)
                        .decodeValue(recoveredValue);

        assertThat(readVersion3ValueTag(recoveredValue)).isEqualTo(200L);
        assertThat(decoded.row.getInt(0)).isEqualTo(1);
        assertThat(decoded.row.getString(1).toString()).isEqualTo("a");
    }

    @Test
    void testRecoveredEventTimeVersion3ValueUsesTimeColumn() {
        Schema schema = eventTimeSchema();
        TableConfig tableConfig = rowTtlEventTimeConfig(schema);
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(schema, DEFAULT_SCHEMA_ID));
        RowTtlTimestampProvider timestampProvider =
                RowTtlTimestampProvider.forRecovery(tableConfig, schemaGetter);
        ValueEncoder writeEncoder =
                ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_3, timestampProvider);
        BinaryRow row = compactedRow(schema.getRowType(), new Object[] {1, 1234L, "a"});

        byte[] recoveredValue =
                KvRecoverHelper.encodeRecoveredValue(
                        writeEncoder, timestampProvider, DEFAULT_SCHEMA_ID, row, 200L);
        BinaryValue decoded =
                new ValueDecoder(schemaGetter, KvFormat.COMPACTED, KV_FORMAT_VERSION_3)
                        .decodeValue(recoveredValue);

        assertThat(readVersion3ValueTag(recoveredValue)).isEqualTo(1234L);
        assertThat(decoded.row.getInt(0)).isEqualTo(1);
        assertThat(decoded.row.getLong(1)).isEqualTo(1234L);
        assertThat(decoded.row.getString(2).toString()).isEqualTo("a");
    }

    private static long readVersion3ValueTag(byte[] value) {
        return KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3)
                .readValueTag(MemorySegment.wrap(value));
    }

    private static TableConfig rowTtlProcessTimeConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.TABLE_KV_ROW_TTL, java.time.Duration.ofHours(1));
        configuration.set(ConfigOptions.TABLE_KV_FORMAT_VERSION, KV_FORMAT_VERSION_3);
        return new TableConfig(configuration);
    }

    private static Schema eventTimeSchema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("event_time", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .build();
    }

    private static TableConfig rowTtlEventTimeConfig(Schema schema) {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.TABLE_KV_ROW_TTL, java.time.Duration.ofHours(1));
        configuration.set(ConfigOptions.TABLE_KV_FORMAT_VERSION, KV_FORMAT_VERSION_3);
        configuration.setString(ConfigOptions.TABLE_KV_ROW_TTL_TIME_COLUMN, "event_time");
        configuration.setString(
                TableConfig.KV_ROW_TTL_TIME_COLUMN_ID_KEY,
                String.valueOf(schema.getColumn("event_time").getColumnId()));
        return new TableConfig(configuration);
    }
}
