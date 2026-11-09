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

import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;

import java.time.ZoneId;
import java.util.List;
import java.util.function.ToLongFunction;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;

/** Provides the timestamp used as the internal KV value tag for row TTL. */
final class RowTtlTimestampProvider implements ToLongFunction<BinaryRow> {

    static final long NEVER_EXPIRE_TIMESTAMP_MS = Long.MAX_VALUE / 2;

    private final TimestampExtractor timestampExtractor;
    private long timestampMs;

    private RowTtlTimestampProvider(TimestampExtractor timestampExtractor) {
        this.timestampExtractor = timestampExtractor;
    }

    static RowTtlTimestampProvider create(
            TableConfig tableConfig, SchemaGetter schemaGetter, ZoneId serverTimeZone) {
        if (tableConfig.getKvTTLTimeColumn().isPresent()) {
            return forEventTime(tableConfig, schemaGetter, serverTimeZone);
        }
        return new RowTtlTimestampProvider(new BatchTimestampExtractor());
    }

    private static RowTtlTimestampProvider forEventTime(
            TableConfig tableConfig, SchemaGetter schemaGetter, ZoneId serverTimeZone) {
        String timeColumn = tableConfig.getKvTTLTimeColumn().get();
        return new RowTtlTimestampProvider(
                EventTimeTimestampExtractor.create(schemaGetter, timeColumn, serverTimeZone));
    }

    void prepareForBatch(long commitTimestamp) {
        timestampMs = commitTimestamp;
    }

    @Override
    public long applyAsLong(BinaryRow row) {
        return timestampExtractor.extract(row, timestampMs);
    }

    private interface TimestampExtractor {
        long extract(BinaryRow row, long timestampMs);
    }

    private static final class BatchTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(BinaryRow row, long timestampMs) {
            return timestampMs;
        }
    }

    private static final class EventTimeTimestampExtractor implements TimestampExtractor {
        private final int fieldIndex;
        private final DataType timeColumnType;
        private final ZoneId serverTimeZone;

        private EventTimeTimestampExtractor(
                int fieldIndex, DataType timeColumnType, ZoneId serverTimeZone) {
            this.fieldIndex = fieldIndex;
            this.timeColumnType = timeColumnType;
            this.serverTimeZone = serverTimeZone;
        }

        private static EventTimeTimestampExtractor create(
                SchemaGetter schemaGetter, String timeColumn, ZoneId serverTimeZone) {
            Schema schema = schemaGetter.getLatestSchemaInfo().getSchema();
            List<Schema.Column> columns = schema.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                Schema.Column column = columns.get(i);
                if (column.getName().equals(timeColumn)) {
                    return new EventTimeTimestampExtractor(i, column.getDataType(), serverTimeZone);
                }
            }
            throw new IllegalStateException(
                    String.format(
                            "Cannot find row TTL time column '%s' in latest schema.", timeColumn));
        }

        @Override
        public long extract(BinaryRow row, long timestampMs) {
            if (row.isNullAt(fieldIndex)) {
                return NEVER_EXPIRE_TIMESTAMP_MS;
            }
            DataTypeRoot typeRoot = timeColumnType.getTypeRoot();
            if (typeRoot == DataTypeRoot.BIGINT) {
                return row.getLong(fieldIndex);
            }
            if (typeRoot == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                return row.getTimestampNtz(fieldIndex, getPrecision(timeColumnType))
                        .toLocalDateTime()
                        .atZone(serverTimeZone)
                        .toInstant()
                        .toEpochMilli();
            }
            if (typeRoot == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                return row.getTimestampLtz(fieldIndex, getPrecision(timeColumnType))
                        .getEpochMillisecond();
            }
            throw new IllegalStateException(
                    String.format("Unsupported row TTL time column type: %s.", timeColumnType));
        }
    }
}
