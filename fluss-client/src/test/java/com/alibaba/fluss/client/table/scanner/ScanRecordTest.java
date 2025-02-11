/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.table.scanner;

import com.alibaba.fluss.record.RowKind;

import org.junit.jupiter.api.Test;

import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ScanRecord}. */
public class ScanRecordTest {

    @Test
    void testBuildSnapshotReadScanRecord() {
        ScanRecord record = new ScanRecord(row(DATA1_ROW_TYPE, new Object[] {1, "a"}));
        assertThat(record.getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(record.logOffset()).isEqualTo(-1L);
        assertThat(record.getRow()).isEqualTo(row(DATA1_ROW_TYPE, new Object[] {1, "a"}));
    }

    @Test
    void testBuildLogScanRecord() {
        ScanRecord record =
                new ScanRecord(
                        1L, 1000L, RowKind.APPEND_ONLY, row(DATA1_ROW_TYPE, new Object[] {1, "a"}));
        assertThat(record.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
        assertThat(record.logOffset()).isEqualTo(1L);
        assertThat(record.getRow()).isEqualTo(row(DATA1_ROW_TYPE, new Object[] {1, "a"}));
    }
}
