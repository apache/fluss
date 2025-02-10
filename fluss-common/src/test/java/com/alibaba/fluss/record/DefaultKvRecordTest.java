/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.TestInternalRowGenerator;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.encode.CompactedRowEncoder;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultKvRecord}. */
class DefaultKvRecordTest extends KvTestBase {

    @Test
    void testBase() throws Exception {
        KvRecordReadContext kvRecordReadContext =
                KvRecordReadContext.createReadContext(KvFormat.COMPACTED, baseRowFieldTypes);
        // create row.

        CompactedRow row;
        try (CompactedRowEncoder writer = new CompactedRowEncoder(baseRowFieldTypes)) {
            writer.startNewRow();
            writer.encodeField(0, 10);
            writer.encodeField(1, BinaryString.fromString("abc"));
            row = writer.finishRow();
        }

        byte[] key = new byte[] {1, 2};
        DefaultKvRecord.writeTo(outputView, key, row);

        // Test read from.
        KvRecord kvRecord =
                DefaultKvRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        schemaId,
                        kvRecordReadContext);

        // four byte for length + bytes for key length  + bytes for key +
        // bytes for row
        // 4 + 1 + 2 + 6 = 13
        assertThat(kvRecord.getSizeInBytes()).isEqualTo(13);
        // check key
        assertThat(keyToBytes(kvRecord)).isEqualTo(key);
        // check value
        assertThat(kvRecord.getRow()).isEqualTo(row);

        // now, check write record with value is null
        key = new byte[] {2, 3};
        outputView = new MemorySegmentOutputView(100);
        DefaultKvRecord.writeTo(outputView, key, null);
        // Test read from.
        kvRecord =
                DefaultKvRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        schemaId,
                        kvRecordReadContext);
        // four byte for length + bytes for key length  + bytes for key
        // 4 + 1 + 2  = 7
        assertThat(kvRecord.getSizeInBytes()).isEqualTo(7);
        // check key
        assertThat(keyToBytes(kvRecord)).isEqualTo(key);
        // check value
        assertThat(kvRecord.getRow()).isNull();
    }

    @Test
    void testWriteToAndReadFromWithRandomData() throws Exception {
        // Test write to.
        CompactedRow row = TestInternalRowGenerator.genCompactedRowForAllType();
        Random rnd = new Random();
        byte[] key = TestInternalRowGenerator.generateRandomBytes(rnd);
        DefaultKvRecord.writeTo(outputView, key, row);

        RowType rowType = TestInternalRowGenerator.createAllRowType();
        DataType[] colTypes = rowType.getChildren().toArray(new DataType[0]);

        // Test read form.
        KvRecord kvRecord =
                DefaultKvRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        schemaId,
                        KvRecordReadContext.createReadContext(KvFormat.COMPACTED, colTypes));

        // check key
        assertThat(keyToBytes(kvRecord)).isEqualTo(key);
        // check value
        assertThat(kvRecord.getRow()).isEqualTo(row);
    }
}
