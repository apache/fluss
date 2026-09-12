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

import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.server.utils.RowTtlUtils;
import org.apache.fluss.utils.clock.Clock;

import org.rocksdb.FlinkCompactionFilter;
import org.rocksdb.RocksDB;

import java.time.Duration;
import java.util.function.LongSupplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Factory utilities for row TTL compaction filters. */
public final class RowTtlCompactionFilterFactory {

    private static final long QUERY_TIME_AFTER_NUM_ENTRIES = 1000L;

    private RowTtlCompactionFilterFactory() {}

    /** Creates a configured native compaction filter factory for row TTL cleanup. */
    public static FlinkCompactionFilter.FlinkCompactionFilterFactory create(
            KvValueLayout kvValueLayout, Duration ttl, Clock clock) {
        long ttlMillis = RowTtlUtils.validateAndConvertTtlDurationToMillis(ttl);
        checkNotNull(clock, "clock must not be null.");
        return create(kvValueLayout, ttlMillis, clock::milliseconds);
    }

    /** Removes values using the default interval for refreshing the supplied current value. */
    static FlinkCompactionFilter.FlinkCompactionFilterFactory create(
            KvValueLayout kvValueLayout,
            long expirationDistance,
            LongSupplier currentValueSupplier) {
        return create(
                kvValueLayout,
                expirationDistance,
                QUERY_TIME_AFTER_NUM_ENTRIES,
                currentValueSupplier);
    }

    /** Removes a value when {@code valueTag + expirationDistance <= currentValue}. */
    static FlinkCompactionFilter.FlinkCompactionFilterFactory create(
            KvValueLayout kvValueLayout,
            long expirationDistance,
            long queryCurrentValueAfterNumEntries,
            LongSupplier currentValueSupplier) {
        checkNotNull(kvValueLayout, "kvValueLayout must not be null.");
        checkNotNull(currentValueSupplier, "currentValueSupplier must not be null.");
        checkArgument(kvValueLayout.hasValueTag(), "Compaction filter requires a tagged layout.");
        checkArgument(expirationDistance >= 0L, "Expiration distance must be non-negative.");
        checkArgument(
                queryCurrentValueAfterNumEntries > 0L,
                "queryCurrentValueAfterNumEntries must be greater than zero.");

        RocksDB.loadLibrary();
        FlinkCompactionFilter.FlinkCompactionFilterFactory factory =
                new FlinkCompactionFilter.FlinkCompactionFilterFactory(
                        currentValueSupplier::getAsLong);
        factory.configure(
                FlinkCompactionFilter.Config.createNotList(
                        FlinkCompactionFilter.StateType.Value,
                        kvValueLayout.valueTagOffset(),
                        expirationDistance,
                        queryCurrentValueAfterNumEntries));
        return factory;
    }
}
