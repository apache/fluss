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
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.TestInternalRowGenerator;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.row.indexed.IndexedRowWriter;
import com.alibaba.fluss.types.DataType;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link IndexedLogRecord}. */
class IndexedLogRecordTest extends LogTestBase {

    @Test
    void testBase() throws IOException {
        DataType[] fieldTypes = baseRowType.getChildren().toArray(new DataType[0]);
        // create row.
        IndexedRow row = new IndexedRow(fieldTypes);
        IndexedRowWriter writer =
                new IndexedRowWriter(baseRowType.getChildren().toArray(new DataType[0]));
        writer.writeInt(10);
        writer.writeString(BinaryString.fromString("abc"));
        row.pointTo(writer.segment(), 0, writer.position());

        IndexedLogRecord.writeTo(outputView, RowKind.APPEND_ONLY, row);
        // Test read from.
        IndexedLogRecord defaultLogRecord =
                IndexedLogRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        1000,
                        10001,
                        fieldTypes);

        assertThat(defaultLogRecord.getSizeInBytes()).isEqualTo(17);
        assertThat(defaultLogRecord.logOffset()).isEqualTo(1000);
        assertThat(defaultLogRecord.timestamp()).isEqualTo(10001);
        assertThat(defaultLogRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
        assertThat(defaultLogRecord.getRow()).isEqualTo(row);
    }

    @Test
    void testWriteToAndReadFromWithRandomData() throws IOException {
        // Test write to.
        IndexedRow row = TestInternalRowGenerator.genIndexedRowForAllType();
        IndexedLogRecord.writeTo(outputView, RowKind.APPEND_ONLY, row);
        DataType[] allColTypes =
                TestInternalRowGenerator.createAllRowType().getChildren().toArray(new DataType[0]);

        // Test read from.
        LogRecord defaultLogRecord =
                IndexedLogRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        1000,
                        10001,
                        allColTypes);

        assertThat(defaultLogRecord.logOffset()).isEqualTo(1000);
        assertThat(defaultLogRecord.timestamp()).isEqualTo(10001);
        assertThat(defaultLogRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
        assertThat(defaultLogRecord.getRow()).isEqualTo(row);
    }
}
