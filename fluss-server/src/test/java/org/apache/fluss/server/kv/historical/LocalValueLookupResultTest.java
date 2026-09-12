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

package org.apache.fluss.server.kv.historical;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.server.kv.KvStateLookupResult;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LocalValueLookupResult}. */
class LocalValueLookupResultTest {

    @Test
    void testCreatesLookupFromLocalAndLakeResults() {
        LocalValueLookupResult localLookupResult = createLookupResult();
        byte[] localValueKey = new byte[] {1};
        byte[] localDeleteKey = new byte[] {2};
        byte[] lakeValueKey = new byte[] {3};
        byte[] lakeMissKey = new byte[] {4};
        BinaryValue localValue = binaryValue(1, "local");
        BinaryValue lakeValue = binaryValue(3, "lake");

        localLookupResult.add(
                localValueKey,
                KvStateLookupResult.present(
                        ValueEncoder.forLayout(KvValueLayout.TAGGED).encodeValue(localValue, 10L)));
        localLookupResult.add(localDeleteKey, KvStateLookupResult.deleted());
        localLookupResult.add(lakeValueKey, KvStateLookupResult.notFound());
        localLookupResult.add(lakeMissKey, KvStateLookupResult.notFound());

        assertThat(localLookupResult.keysMissingLocally())
                .containsExactly(lakeValueKey, lakeMissKey);

        HistoricalValueLookup valueLookup =
                localLookupResult.createValueLookup(
                        Arrays.asList(
                                ValueEncoder.forLayout(KvValueLayout.PLAIN).encodeValue(lakeValue),
                                null));

        assertThat(valueLookup.lookup(localValueKey)).isEqualTo(localValue);
        assertThat(valueLookup.lookup(localDeleteKey)).isNull();
        assertThat(valueLookup.lookup(lakeValueKey)).isEqualTo(lakeValue);
        assertThat(valueLookup.lookup(lakeMissKey)).isNull();
        assertThatThrownBy(() -> valueLookup.lookup(new byte[] {5}))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("No previous value for a historical write key");
    }

    @Test
    void testValidatesLakeResultCountBeforeCreatingLookup() {
        LocalValueLookupResult localLookupResult = createLookupResult();
        localLookupResult.add(new byte[] {1}, KvStateLookupResult.notFound());

        assertThatThrownBy(() -> localLookupResult.createValueLookup(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected 1 historical lake values, but received 0");
    }

    private static LocalValueLookupResult createLookupResult() {
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA);
        return new LocalValueLookupResult(
                new ValueDecoder(schemaGetter, KvFormat.COMPACTED, KvValueLayout.TAGGED),
                new ValueDecoder(schemaGetter, KvFormat.COMPACTED, KvValueLayout.PLAIN));
    }

    private static BinaryValue binaryValue(int id, String value) {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {id, value});
        return new BinaryValue(DEFAULT_SCHEMA_ID, row);
    }
}
