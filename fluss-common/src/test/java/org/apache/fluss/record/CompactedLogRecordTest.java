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

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.types.DataType;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompactedLogRecord}. */
class CompactedLogRecordTest extends LogTestBase {

    @Test
    void testBase() throws IOException {
        DataType[] fieldTypes = baseRowType.getChildren().toArray(new DataType[0]);

        CompactedRowWriter writer = new CompactedRowWriter(fieldTypes.length);
        // field 0: int 10
        writer.writeInt(10);
        // field 1: string "abc"
        writer.writeString(BinaryString.fromString("abc"));
        byte[] bytes = writer.toBytes();
        CompactedRow row =
                CompactedRow.from(fieldTypes, bytes, new CompactedRowDeserializer(fieldTypes));

        CompactedLogRecord.writeTo(outputView, ChangeType.APPEND_ONLY, row);

        CompactedLogRecord logRecord =
                CompactedLogRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        1000,
                        10001,
                        fieldTypes);

        assertThat(logRecord.getSizeInBytes()).isEqualTo(1 + row.getSizeInBytes() + 4);
        assertThat(logRecord.logOffset()).isEqualTo(1000);
        assertThat(logRecord.timestamp()).isEqualTo(10001);
        assertThat(logRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
        assertThat(logRecord.getRow()).isEqualTo(row);
    }

    @Test
    void testWriteToAndReadFromWithRandomData() throws IOException {
        // generate a compacted row for all supported types
        DataType[] allColTypes =
                TestInternalRowGenerator.createAllRowType().getChildren().toArray(new DataType[0]);
        CompactedRow row = TestInternalRowGenerator.genCompactedRowForAllType();

        CompactedLogRecord.writeTo(outputView, ChangeType.APPEND_ONLY, row);

        LogRecord logRecord =
                CompactedLogRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        1000,
                        10001,
                        allColTypes);

        assertThat(logRecord.logOffset()).isEqualTo(1000);
        assertThat(logRecord.timestamp()).isEqualTo(10001);
        assertThat(logRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
        assertThat(logRecord.getRow()).isEqualTo(row);
    }
}
