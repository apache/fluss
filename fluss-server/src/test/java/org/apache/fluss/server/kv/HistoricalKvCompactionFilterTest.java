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

import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.rocksdb.RocksDBHandle;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.server.kv.historical.HistoricalKvTombstone;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlinkCompactionFilter;
import org.rocksdb.FlushOptions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests offset-distance retention for historical values and tombstones. */
class HistoricalKvCompactionFilterTest {

    @TempDir private Path tempDir;

    @ParameterizedTest
    @ValueSource(longs = {0L, 60_000L})
    void testRemovesOnlyOffsetsBeyondRetentionDistance(long retentionOffsetDistance)
            throws Exception {
        AtomicLong cleanupOffset = new AtomicLong(0L);
        byte[] valueBeforeKey = bytes("value-before");
        byte[] tombstoneBeforeKey = bytes("tombstone-before");
        byte[] valueAtKey = bytes("value-at");
        byte[] tombstoneAtKey = bytes("tombstone-at");
        byte[] valueAfterKey = bytes("value-after");
        byte[] tombstoneAfterKey = bytes("tombstone-after");
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});

        try (FlinkCompactionFilter.FlinkCompactionFilterFactory filterFactory =
                        RowTtlCompactionFilterFactory.create(
                                KvValueLayout.TAGGED,
                                retentionOffsetDistance,
                                1L,
                                () -> cleanupOffset.get() - 1L);
                DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
                ColumnFamilyOptions cfOptions =
                        new ColumnFamilyOptions().setCompactionFilterFactory(filterFactory);
                RocksDBHandle handle = new RocksDBHandle(tempDir.toFile(), dbOptions, cfOptions);
                FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            handle.openDB();
            handle.getDb().put(valueBeforeKey, encodeValue(row, 4L));
            handle.getDb().put(tombstoneBeforeKey, HistoricalKvTombstone.encode(4L));
            handle.getDb().put(valueAtKey, encodeValue(row, 5L));
            handle.getDb().put(tombstoneAtKey, HistoricalKvTombstone.encode(5L));
            handle.getDb().put(valueAfterKey, encodeValue(row, 6L));
            handle.getDb().put(tombstoneAfterKey, HistoricalKvTombstone.encode(6L));
            handle.getDb().flush(flushOptions);

            handle.getDb().compactRange();
            assertThat(handle.getDb().get(valueBeforeKey)).isNotNull();
            assertThat(handle.getDb().get(tombstoneBeforeKey)).isNotNull();

            // At this boundary, offset 4 is still inside the retained range.
            cleanupOffset.set(retentionOffsetDistance + 4L);
            handle.getDb().compactRange();
            assertThat(handle.getDb().get(valueBeforeKey)).isNotNull();
            assertThat(handle.getDb().get(tombstoneBeforeKey)).isNotNull();

            // Advancing by one makes offset 4 eligible, while offsets 5 and 6 remain retained.
            cleanupOffset.set(retentionOffsetDistance + 5L);
            handle.getDb().compactRange();

            assertThat(handle.getDb().get(valueBeforeKey)).isNull();
            assertThat(handle.getDb().get(tombstoneBeforeKey)).isNull();
            assertThat(handle.getDb().get(valueAtKey)).isNotNull();
            assertThat(handle.getDb().get(tombstoneAtKey)).isNotNull();
            assertThat(handle.getDb().get(valueAfterKey)).isNotNull();
            assertThat(handle.getDb().get(tombstoneAfterKey)).isNotNull();
        }
    }

    private static byte[] encodeValue(BinaryRow row, long logOffset) {
        return ValueEncoder.forLayout(KvValueLayout.TAGGED)
                .encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row), logOffset);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
