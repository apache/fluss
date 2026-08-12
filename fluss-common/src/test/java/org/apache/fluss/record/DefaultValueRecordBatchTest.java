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

package org.apache.fluss.record;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultValueRecordBatch}. */
class DefaultValueRecordBatchTest {

    @Test
    void testEqualsAndHashCode() throws Exception {
        DefaultValueRecordBatch batch1 = buildBatch();
        DefaultValueRecordBatch batch2 = buildBatch();

        assertThat(batch1).isEqualTo(batch2);
        assertThat(batch1).hasSameHashCodeAs(batch2);
    }

    @Test
    void testHashCodeDiffersForDifferentContents() throws Exception {
        DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();
        builder.append(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a1"}));
        DefaultValueRecordBatch batch1 = builder.build();

        DefaultValueRecordBatch.Builder otherBuilder = DefaultValueRecordBatch.builder();
        otherBuilder.append(
                DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {2, "a2"}));
        DefaultValueRecordBatch batch2 = otherBuilder.build();

        assertThat(batch1).isNotEqualTo(batch2);
        assertThat(batch1.hashCode()).isNotEqualTo(batch2.hashCode());
    }

    @Test
    void testEmptyBatchesAreEqualAndShareHashCode() throws Exception {
        DefaultValueRecordBatch batch1 = DefaultValueRecordBatch.builder().build();
        DefaultValueRecordBatch batch2 = DefaultValueRecordBatch.builder().build();

        assertThat(batch1).isEqualTo(batch2);
        assertThat(batch1).hasSameHashCodeAs(batch2);
    }

    private static DefaultValueRecordBatch buildBatch() throws Exception {
        DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();
        builder.append(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a1"}));
        builder.append(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {2, "a2"}));
        return builder.build();
    }
}
