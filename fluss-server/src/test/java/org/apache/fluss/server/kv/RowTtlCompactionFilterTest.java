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
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.rocksdb.RocksDBHandle;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlinkCompactionFilter;
import org.rocksdb.FlushOptions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests row TTL cleanup with native Flink compaction filter. */
class RowTtlCompactionFilterTest {

    @TempDir private Path tempDir;

    @Test
    void testFlinkCompactionFilterReadsTimestampFromVersion3Value() throws Exception {
        FlinkCompactionFilter.FlinkCompactionFilterFactory filterFactory =
                RowTtlCompactionFilterFactory.create(
                        Duration.ofHours(1L), 1L, new ManualClock(123456789L));
        byte[] expiredKey = "expired-key".getBytes(StandardCharsets.UTF_8);
        byte[] freshKey = "fresh-key".getBytes(StandardCharsets.UTF_8);
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        long now = 123456789L;

        try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
                ColumnFamilyOptions cfOptions =
                        new ColumnFamilyOptions().setCompactionFilterFactory(filterFactory);
                RocksDBHandle handle = new RocksDBHandle(tempDir.toFile(), dbOptions, cfOptions);
                FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            handle.openDB();
            handle.getDb()
                    .put(
                            expiredKey,
                            encodeVersion3Value(row, now - Duration.ofHours(2L).toMillis()));
            handle.getDb().put(freshKey, encodeVersion3Value(row, now));
            handle.getDb().flush(flushOptions);

            handle.getDb().compactRange();

            assertThat(handle.getDb().get(expiredKey)).isNull();
            assertThat(handle.getDb().get(freshKey)).isNotNull();
        }
    }

    @Test
    void testCreateRejectsInvalidTtlDuration() {
        assertThatThrownBy(
                        () ->
                                RowTtlCompactionFilterFactory.create(
                                        Duration.ZERO, 1L, new ManualClock(0L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_ROW_TTL.key());
    }

    private static byte[] encodeVersion3Value(BinaryRow row, long valueTag) {
        return ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_3, ignored -> valueTag)
                .encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row));
    }
}
