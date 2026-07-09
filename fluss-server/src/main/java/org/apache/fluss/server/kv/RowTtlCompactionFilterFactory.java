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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.row.encode.ValueLayout;
import org.apache.fluss.server.utils.RowTtlUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.rocksdb.FlinkCompactionFilter;
import org.rocksdb.RocksDB;

import java.time.Duration;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Factory utilities for row TTL compaction filters. */
public final class RowTtlCompactionFilterFactory {

    private static final long QUERY_TIME_AFTER_NUM_ENTRIES = 1000L;

    private RowTtlCompactionFilterFactory() {}

    /** Creates a configured native compaction filter factory for row TTL cleanup. */
    public static FlinkCompactionFilter.FlinkCompactionFilterFactory create(Duration ttl) {
        return create(ttl, QUERY_TIME_AFTER_NUM_ENTRIES, SystemClock.getInstance());
    }

    /** Creates a configured native compaction filter factory for row TTL cleanup. */
    public static FlinkCompactionFilter.FlinkCompactionFilterFactory create(
            Duration ttl, Clock clock) {
        return create(ttl, QUERY_TIME_AFTER_NUM_ENTRIES, clock);
    }

    @VisibleForTesting
    static FlinkCompactionFilter.FlinkCompactionFilterFactory create(
            Duration ttl, long queryTimeAfterNumEntries) {
        return create(ttl, queryTimeAfterNumEntries, SystemClock.getInstance());
    }

    @VisibleForTesting
    static FlinkCompactionFilter.FlinkCompactionFilterFactory create(
            Duration ttl, long queryTimeAfterNumEntries, Clock clock) {
        long ttlMillis = RowTtlUtils.validateAndCeilTtlDurationToMillis(ttl);
        checkNotNull(clock, "clock must not be null.");
        checkArgument(
                queryTimeAfterNumEntries > 0,
                "queryTimeAfterNumEntries must be greater than zero.");

        RocksDB.loadLibrary();
        FlinkCompactionFilter.FlinkCompactionFilterFactory factory =
                new FlinkCompactionFilter.FlinkCompactionFilterFactory(clock::milliseconds);
        factory.configure(
                FlinkCompactionFilter.Config.createNotList(
                        FlinkCompactionFilter.StateType.Value,
                        ValueLayout.forVersion(KV_FORMAT_VERSION_3).valueTimestampOffset(),
                        ttlMillis,
                        queryTimeAfterNumEntries));
        return factory;
    }
}
