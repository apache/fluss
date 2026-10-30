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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.server.kv.historical.HistoricalKvTombstone;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.server.kv.rocksdb.RocksDBResourceContainer;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KvStateAccessor}. */
class KvStateAccessorTest {

    private static final ValueEncoder VALUE_ENCODER = ValueEncoder.forLayout(KvValueLayout.TAGGED);

    private final ValueDecoder valueDecoder =
            new ValueDecoder(
                    new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA),
                    KvFormat.COMPACTED,
                    KvValueLayout.TAGGED);

    private @TempDir File rocksDBDir;
    private RocksDBKv rocksDBKv;
    private KvStateAccessor stateAccessor;
    private Key key;

    @BeforeEach
    void beforeEach() throws Exception {
        RocksDBResourceContainer resourceContainer =
                new RocksDBResourceContainer(new Configuration(), rocksDBDir);
        rocksDBKv =
                new RocksDBKvBuilder(
                                rocksDBDir, resourceContainer, resourceContainer.getColumnOptions())
                        .build();
        stateAccessor =
                new KvStateAccessor(
                        new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS),
                        rocksDBKv,
                        true);
        key = stateAccessor.encodeKey(new byte[] {1}, "20240107");
    }

    @AfterEach
    void afterEach() {
        IOUtils.closeQuietly(rocksDBKv);
    }

    @Test
    void testReusesSavedValueWithoutRocksDBLookup() throws Exception {
        // A RocksDB read would return this value instead of the saved one.
        rocksDBKv.put(key.get(), VALUE_ENCODER.encodeValue(binaryValue("rocksdb"), 0L));
        BinaryValue savedValue = binaryValue("saved");

        assertThat(stateAccessor.lookup(key, valueDecoder, () -> savedValue)).isSameAs(savedValue);
        assertThat(stateAccessor.lookup(key, valueDecoder, () -> null)).isNull();
    }

    @Test
    void testBufferedUpdateAndDeleteOverrideSavedValue() throws Exception {
        rocksDBKv.put(key.get(), VALUE_ENCODER.encodeValue(binaryValue("rocksdb"), 0L));
        Supplier<BinaryValue> savedLookup =
                () -> {
                    throw new AssertionError(
                            "A buffered mutation must stop fallback to saved state");
                };
        BinaryValue updatedValue = binaryValue("updated");
        stateAccessor.update(key, VALUE_ENCODER.encodeValue(updatedValue, 1L), 1L);
        assertThat(stateAccessor.lookup(key, valueDecoder, savedLookup)).isEqualTo(updatedValue);
        assertThat(stateAccessor.lookup(key, valueDecoder, null)).isEqualTo(updatedValue);

        // A delete must mask both the saved and the RocksDB value so a later record cannot
        // resurrect it.
        stateAccessor.delete(key, 2L);
        assertThat(stateAccessor.lookup(key, valueDecoder, savedLookup)).isNull();
        assertThat(stateAccessor.lookup(key, valueDecoder, null)).isNull();
    }

    @Test
    void testLocalLookupPreservesMissingAndDeletedStates() throws Exception {
        BinaryValue value = binaryValue("local");

        // The local probe must schedule lake lookup only for a true miss, not a tombstone.
        assertThat(stateAccessor.lookupLocal(key)).isEqualTo(KvStateLookupResult.notFound());
        rocksDBKv.put(key.get(), HistoricalKvTombstone.encode(0L));
        assertThat(stateAccessor.lookupLocal(key)).isEqualTo(KvStateLookupResult.deleted());
        rocksDBKv.put(key.get(), VALUE_ENCODER.encodeValue(value, 1L));
        assertThat(stateAccessor.lookup(key, valueDecoder, null)).isEqualTo(value);
    }

    @Test
    void testPrefetchExcludesBufferedValuesAndDeletesAndPreservesMisses() throws Exception {
        Key buffered = Key.of(new byte[] {2});
        Key deleted = Key.of(new byte[] {3});
        Key missing = Key.of(new byte[] {4});
        byte[] stored = new byte[] {10};
        rocksDBKv.put(key.get(), stored);
        // RocksDB values of buffered keys must not be prefetched: the buffer determines them.
        rocksDBKv.put(buffered.get(), new byte[] {11});
        rocksDBKv.put(deleted.get(), new byte[] {12});
        stateAccessor.update(buffered, new byte[] {20}, 0L);
        stateAccessor.delete(deleted, 1L);

        Map<Key, byte[]> values =
                stateAccessor.prefetchRocksDBValues(
                        new LinkedHashSet<>(Arrays.asList(key, buffered, deleted, missing, key)));

        assertThat(values).containsOnlyKeys(key, missing);
        assertThat(values.get(key)).isEqualTo(stored);
        assertThat(values).containsEntry(missing, null);
    }

    private static BinaryValue binaryValue(String value) {
        return new BinaryValue(
                DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {1, value}));
    }
}
