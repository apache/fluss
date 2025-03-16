/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.ChangeType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ScanRecords}. */
public class ScanRecordsTest {
    @Test
    void iterator() {
        Map<TableBucket, List<ScanRecord>> records = new LinkedHashMap<>();
        long tableId = 0;
        records.put(new TableBucket(tableId, 0), new ArrayList<>());
        ScanRecord record1 = new ScanRecord(0L, 1000L, ChangeType.INSERT, row(1, "a"));
        ScanRecord record2 = new ScanRecord(1L, 1000L, ChangeType.UPDATE_BEFORE, row(1, "a"));
        ScanRecord record3 = new ScanRecord(2L, 1000L, ChangeType.UPDATE_AFTER, row(1, "a1"));
        ScanRecord record4 = new ScanRecord(3L, 1000L, ChangeType.DELETE, row(1, "a1"));
        records.put(new TableBucket(tableId, 1), Arrays.asList(record1, record2, record3, record4));
        records.put(new TableBucket(tableId, 2), new ArrayList<>());

        ScanRecords scanRecords = new ScanRecords(records);
        Iterator<ScanRecord> iter = scanRecords.iterator();

        int c = 0;
        for (; iter.hasNext(); c++) {
            ScanRecord record = iter.next();
            assertThat(record.logOffset()).isEqualTo(c);
        }
        assertThat(c).isEqualTo(4);
    }
}
