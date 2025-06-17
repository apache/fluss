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

package com.alibaba.fluss.record;

import com.alibaba.fluss.memory.UnmanagedPagedOutputView;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.row.TestInternalRowGenerator;
import com.alibaba.fluss.row.compacted.CompactedRow;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultKvRecordBatch}. */
class DefaultKvRecordBatchTest extends KvTestBase {

    @Test
    void writeAndReadBatch() throws Exception {
        int recordNumber = 100;
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED);

        List<byte[]> keys = new ArrayList<>();
        List<CompactedRow> rows = new ArrayList<>();
        for (int i = 0; i < recordNumber; i++) {
            byte[] key = new byte[] {(byte) i, (byte) i};
            CompactedRow row =
                    i % 2 == 1 ? null : TestInternalRowGenerator.genCompactedRowForAllType();
            builder.append(key, row);
            keys.add(key);
            rows.add(row);
        }

        KvRecordBatch kvRecords = DefaultKvRecordBatch.pointToBytesView(builder.build());
        kvRecords.ensureValid();

        // verify the header info
        assertThat(kvRecords.getRecordCount()).isEqualTo(recordNumber);
        assertThat(kvRecords.magic()).isEqualTo(magic);
        assertThat(kvRecords.isValid()).isTrue();
        assertThat(kvRecords.schemaId()).isEqualTo(schemaId);

        // verify record.
        int i = 0;
        for (KvRecord record :
                kvRecords.records(
                        KvRecordReadContext.createReadContext(
                                KvFormat.COMPACTED, baseRowFieldTypes))) {
            assertThat(keyToBytes(record)).isEqualTo(keys.get(i));
            assertThat(record.getRow()).isEqualTo(rows.get(i));
            i++;
        }

        builder.close();
    }
}
