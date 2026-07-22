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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
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

    /**
     * Verifies buckets(), isEmpty(), and consumedUpToOffset() semantics for progress-only polls.
     */
    @Test
    void bucketsAndIsEmptySemantics() {
        TableBucket tb = new TableBucket(0L, 0);

        // No records and no progress: both isEmpty() and buckets() must be empty.
        ScanRecords trulyEmpty = ScanRecords.EMPTY;
        assertThat(trulyEmpty.isEmpty()).isTrue();
        assertThat(trulyEmpty.hasProgress()).isFalse();
        assertThat(trulyEmpty.buckets()).isEmpty();

        // Progress-only round: isEmpty() stays true (no materialized records),
        // but buckets() exposes the advanced buckets and consumedUpToOffset carries the offset.
        TableBucket emptyBucket = new TableBucket(0L, 1);
        Map<TableBucket, List<ScanRecord>> progressRecords = new HashMap<>();
        progressRecords.put(tb, Collections.emptyList());
        progressRecords.put(emptyBucket, Collections.emptyList());
        Map<TableBucket, Long> progressOffsets = new HashMap<>();
        progressOffsets.put(tb, 42L);
        progressOffsets.put(emptyBucket, 10L);
        ScanRecords progressOnly = new ScanRecords(progressRecords, progressOffsets);
        assertThat(progressOnly.isEmpty()).isTrue();
        assertThat(progressOnly.hasProgress()).isTrue();
        assertThat(progressOnly.buckets()).containsExactlyInAnyOrder(tb, emptyBucket);
        assertThat(progressOnly.records(emptyBucket)).isEmpty();
        assertThat(progressOnly.consumedUpToOffset(tb)).isEqualTo(42L);
        assertThat(progressOnly.consumedUpToOffset(emptyBucket)).isEqualTo(10L);
        assertThat(progressOnly.consumedUpToOffset(new TableBucket(0L, 99))).isNull();

        // Materialized records present: isEmpty() flips to false;
        // the legacy single-arg constructor has no consumedUpToOffset.
        Map<TableBucket, List<ScanRecord>> matRecords = new HashMap<>();
        matRecords.put(
                tb,
                Collections.singletonList(
                        new ScanRecord(0L, 1000L, ChangeType.INSERT, row(1, "a"))));
        ScanRecords withRecords = new ScanRecords(matRecords);
        assertThat(withRecords.isEmpty()).isFalse();
        assertThat(withRecords.hasProgress()).isTrue();
        assertThat(withRecords.buckets()).containsExactly(tb);
        assertThat(withRecords.consumedUpToOffset(tb)).isNull();
    }
}
