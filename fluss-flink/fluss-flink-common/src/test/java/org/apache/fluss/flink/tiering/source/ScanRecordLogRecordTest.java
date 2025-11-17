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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the private adapter TieringSplitReader.ScanRecordLogRecord */
class ScanRecordLogRecordTest {

    @Test
    void testAdapterDelegatesAllMethods() throws Exception {
        // Prepare a simple InternalRow and wrap it in a ScanRecord with known attributes
        GenericRow row = new GenericRow(1);
        row.setField(0, 42); // content is irrelevant for this test

        long offset = 1234L;
        long timestamp = 987654321L;
        ChangeType changeType = ChangeType.UPDATE_AFTER;
        ScanRecord<InternalRow> scanRecord = new ScanRecord<>(offset, timestamp, changeType, row);

        // Reflectively construct the private static inner class
        Class<?> clazz =
                Class.forName(
                        "org.apache.fluss.flink.tiering.source.TieringSplitReader$ScanRecordLogRecord");
        @SuppressWarnings("unchecked")
        Constructor<? extends LogRecord> ctor =
                (Constructor<? extends LogRecord>) clazz.getDeclaredConstructor(ScanRecord.class);
        ctor.setAccessible(true);
        LogRecord adapter = ctor.newInstance(scanRecord);

        // Verify delegation
        assertThat(adapter.logOffset()).isEqualTo(offset);
        assertThat(adapter.timestamp()).isEqualTo(timestamp);
        assertThat(adapter.getChangeType()).isEqualTo(changeType);
        assertThat(adapter.getRow()).isSameAs(row);

        // Call twice to make sure repeated invocations are fine and to bump coverage
        assertThat(adapter.logOffset()).isEqualTo(offset);
        assertThat(adapter.getRow()).isSameAs(row);
    }
}
