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
import org.apache.fluss.utils.clock.Clock;

import java.util.List;
import java.util.Optional;
import java.util.function.ToLongFunction;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Provides the timestamp used as the V3 KV value tag for row TTL. */
final class RowTtlTimestampProvider implements ToLongFunction<BinaryRow> {

    static final long NEVER_EXPIRE_TIMESTAMP_MS = Long.MAX_VALUE / 2;

    private final TimestampExtractor timestampExtractor;
    private long timestampMs;

    private RowTtlTimestampProvider(TimestampExtractor timestampExtractor) {
        this.timestampExtractor = timestampExtractor;
    }

    static RowTtlTimestampProvider forWrite(
            TableConfig tableConfig, SchemaGetter schemaGetter, Clock clock) {
        checkNotNull(clock, "clock must not be null.");
        if (tableConfig.getRowTTLTimeColumn().isPresent()) {
            return forEventTime(tableConfig, schemaGetter);
        }
        return new RowTtlTimestampProvider(new BatchClockTimestampExtractor(clock));
    }

    static RowTtlTimestampProvider forRecovery(TableConfig tableConfig, SchemaGetter schemaGetter) {
        if (tableConfig.getRowTTLTimeColumn().isPresent()) {
            return forEventTime(tableConfig, schemaGetter);
        }
        return new RowTtlTimestampProvider(new LogRecordTimestampExtractor());
    }

    private static RowTtlTimestampProvider forEventTime(
            TableConfig tableConfig, SchemaGetter schemaGetter) {
        Optional<Integer> timeColumnId = tableConfig.getRowTTLTimeColumnId();
        checkState(
                timeColumnId.isPresent(), "Event-time row TTL requires internal time column id.");
        return new RowTtlTimestampProvider(
                EventTimeTimestampExtractor.create(schemaGetter, timeColumnId.get()));
    }

    void prepareForWriteBatch() {
        timestampExtractor.prepareForWriteBatch(this);
    }

    void setLogRecordTimestampMs(long timestampMs) {
        timestampExtractor.setLogRecordTimestampMs(this, timestampMs);
    }

    @Override
    public long applyAsLong(BinaryRow row) {
        return timestampExtractor.extract(row, timestampMs);
    }

    private interface TimestampExtractor {
        long extract(BinaryRow row, long timestampMs);

        default void prepareForWriteBatch(RowTtlTimestampProvider provider) {}

        default void setLogRecordTimestampMs(RowTtlTimestampProvider provider, long timestampMs) {}
    }

    private static final class BatchClockTimestampExtractor implements TimestampExtractor {
        private final Clock clock;

        private BatchClockTimestampExtractor(Clock clock) {
            this.clock = clock;
        }

        @Override
        public long extract(BinaryRow row, long timestampMs) {
            return timestampMs;
        }

        @Override
        public void prepareForWriteBatch(RowTtlTimestampProvider provider) {
            provider.timestampMs = clock.milliseconds();
        }
    }

    private static final class LogRecordTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(BinaryRow row, long timestampMs) {
            return timestampMs;
        }

        @Override
        public void setLogRecordTimestampMs(RowTtlTimestampProvider provider, long timestampMs) {
            provider.timestampMs = timestampMs;
        }
    }

    private static final class EventTimeTimestampExtractor implements TimestampExtractor {
        private final int fieldIndex;
        private final DataType timeColumnType;

        private EventTimeTimestampExtractor(int fieldIndex, DataType timeColumnType) {
            this.fieldIndex = fieldIndex;
            this.timeColumnType = timeColumnType;
        }

        private static EventTimeTimestampExtractor create(
                SchemaGetter schemaGetter, int timeColumnId) {
            Schema schema = schemaGetter.getLatestSchemaInfo().getSchema();
            List<Schema.Column> columns = schema.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                Schema.Column column = columns.get(i);
                if (column.getColumnId() == timeColumnId) {
                    return new EventTimeTimestampExtractor(i, column.getDataType());
                }
            }
            throw new IllegalStateException(
                    String.format(
                            "Cannot find row TTL time column id %s in latest schema.",
                            timeColumnId));
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
            if (typeRoot == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                return row.getTimestampLtz(fieldIndex, getPrecision(timeColumnType))
                        .getEpochMillisecond();
            }
            throw new IllegalStateException(
                    String.format("Unsupported row TTL time column type: %s.", timeColumnType));
        }
    }
}
