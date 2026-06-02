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

import org.apache.fluss.client.metadata.TestingClientSchemaGetter;
import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ChunkedAllocationManager;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LakeCompletedFetch}. */
class LakeCompletedFetchTest {

    @Test
    void testFilterLakeRecordsByRequestedOffsetRange() {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        LogScannerStatus status = new LogScannerStatus();
        status.assignScanBuckets(Collections.singletonMap(tableBucket, 5L));

        List<LogRecord> records = Arrays.asList(record(3L), record(5L), record(6L), record(7L));
        LogRecordReadContext readContext =
                LogRecordReadContext.createReadContext(
                        DATA1_TABLE_INFO,
                        false,
                        null,
                        new TestingClientSchemaGetter(
                                DATA1_TABLE_PATH,
                                new SchemaInfo(DATA1_SCHEMA, 1),
                                new TestingMetadataUpdater(
                                        Collections.singletonMap(
                                                DATA1_TABLE_PATH, DATA1_TABLE_INFO)),
                                new Configuration()),
                        new ChunkedAllocationManager.ChunkedFactory());

        LakeCompletedFetch fetch =
                new LakeCompletedFetch(
                        tableBucket,
                        CloseableIterator.wrap(records.iterator()),
                        5L,
                        7L,
                        10L,
                        readContext,
                        status);

        List<ScanRecord> scanRecords = fetch.fetchRecords(10);

        assertThat(scanRecords).extracting(ScanRecord::logOffset).containsExactly(5L, 6L);
        assertThat(fetch.nextFetchOffset()).isEqualTo(7L);
        assertThat(fetch.isConsumed()).isTrue();
    }

    private static ScanRecord record(long offset) {
        return new ScanRecord(offset, offset, ChangeType.APPEND_ONLY, new GenericRow(2));
    }
}
