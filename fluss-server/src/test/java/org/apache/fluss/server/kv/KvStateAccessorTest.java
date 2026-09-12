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
import org.apache.fluss.server.metrics.group.TestingMetricGroups;

import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests for {@link KvStateAccessor}. */
class KvStateAccessorTest {

    private final RocksDBKv rocksDBKv = mock(RocksDBKv.class);
    private final KvStateAccessor stateAccessor =
            new KvStateAccessor(
                    new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS),
                    rocksDBKv,
                    true);
    private final ValueDecoder valueDecoder =
            new ValueDecoder(
                    new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA),
                    KvFormat.COMPACTED,
                    KvValueLayout.TAGGED);
    private final Key key = stateAccessor.encodeKey(new byte[] {1}, "20240107");

    @Test
    void testReusesSavedValueWithoutRocksDBLookup() throws Exception {
        BinaryValue savedValue = binaryValue("saved");

        assertThat(stateAccessor.lookup(key, valueDecoder, () -> savedValue)).isSameAs(savedValue);
        assertThat(stateAccessor.lookup(key, valueDecoder, () -> null)).isNull();
        verifyNoInteractions(rocksDBKv);
    }

    @Test
    void testBufferedUpdateAndDeleteOverrideSavedValue() throws Exception {
        Supplier<BinaryValue> savedLookup =
                () -> {
                    throw new AssertionError(
                            "A buffered mutation must stop fallback to saved state");
                };
        BinaryValue updatedValue = binaryValue("updated");
        stateAccessor.update(
                key,
                ValueEncoder.forLayout(KvValueLayout.TAGGED).encodeValue(updatedValue, 0L),
                0L);
        assertThat(stateAccessor.lookup(key, valueDecoder, savedLookup)).isEqualTo(updatedValue);
        assertThat(stateAccessor.lookup(key, valueDecoder, null)).isEqualTo(updatedValue);

        // A delete must mask the saved value so a later record cannot resurrect it.
        stateAccessor.delete(key, 1L);
        assertThat(stateAccessor.lookup(key, valueDecoder, savedLookup)).isNull();
        assertThat(stateAccessor.lookup(key, valueDecoder, null)).isNull();
        verifyNoInteractions(rocksDBKv);
    }

    @Test
    void testLocalLookupPreservesMissingAndDeletedStates() throws Exception {
        BinaryValue value = binaryValue("local");
        byte[] encodedValue = ValueEncoder.forLayout(KvValueLayout.TAGGED).encodeValue(value, 1L);
        when(rocksDBKv.get(key.get()))
                .thenReturn(null, HistoricalKvTombstone.encode(0L), encodedValue);

        // The local probe must schedule lake lookup only for a true miss, not a tombstone.
        assertThat(stateAccessor.lookupLocal(key)).isEqualTo(KvStateLookupResult.notFound());
        assertThat(stateAccessor.lookupLocal(key)).isEqualTo(KvStateLookupResult.deleted());
        assertThat(stateAccessor.lookup(key, valueDecoder, null)).isEqualTo(value);
    }

    private static BinaryValue binaryValue(String value) {
        return new BinaryValue(
                DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {1, value}));
    }
}
