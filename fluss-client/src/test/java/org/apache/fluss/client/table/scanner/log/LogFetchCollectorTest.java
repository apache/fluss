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

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogFetchCollector}. */
public class LogFetchCollectorTest {
    private LogScannerStatus logScannerStatus;
    private LogFetchBuffer logFetchBuffer;
    private LogFetchCollector logFetchCollector;
    private LogRecordReadContext readContext;

    @BeforeEach
    void setup() {
        MetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));
        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        scanBuckets.put(new TableBucket(DATA1_TABLE_ID, 0), 0L);
        scanBuckets.put(new TableBucket(DATA1_TABLE_ID, 1), 0L);
        scanBuckets.put(new TableBucket(DATA1_TABLE_ID, 2), 0L);
        logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(scanBuckets);
        logFetchBuffer = new LogFetchBuffer();
        logFetchCollector =
                new LogFetchCollector(
                        DATA1_TABLE_PATH, logScannerStatus, new Configuration(), metadataUpdater);
        readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER);
    }

    @AfterEach
    void afterEach() {
        if (readContext != null) {
            readContext.close();
            readContext = null;
        }
    }

    @Test
    void testNormal() throws Exception {
        long fetchOffset = 0L;
        int bucketId = 0; // records for 0-10.
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, bucketId);
        FetchLogResultForBucket resultForBucket0 =
                new FetchLogResultForBucket(tb, genMemoryLogRecordsByObject(DATA1), 10L);
        CompletedFetch completedFetch = makeCompletedFetch(tb, resultForBucket0, fetchOffset);

        // Validate that the buffer is empty until after we add the fetch data.
        assertThat(logFetchBuffer.isEmpty()).isTrue();
        logFetchBuffer.add(completedFetch);
        assertThat(logFetchBuffer.isEmpty()).isFalse();

        // Validate that the completed fetch isn't initialized just because we add it to the buffer
        assertThat(completedFetch.isInitialized()).isFalse();

        // Fetch the data and validate that we get all the records we want back.
        Map<TableBucket, List<ScanRecord>> bucketAndRecords =
                logFetchCollector.collectFetch(logFetchBuffer);
        assertThat(bucketAndRecords.size()).isEqualTo(1);
        assertThat(bucketAndRecords.get(tb)).size().isEqualTo(10);

        // When we collected the data from the buffer, this will cause the completed fetch to get
        // initialized.
        assertThat(completedFetch.isInitialized()).isTrue();

        assertThat(completedFetch.isConsumed()).isTrue();

        assertThat(logFetchBuffer.isEmpty()).isTrue();
        assertThat(logFetchBuffer.peek()).isNull();
        assertThat(logFetchBuffer.poll()).isNull();

        // However, while the queue is "empty", the next-in-line fetch is actually still in the
        // buffer.
        assertThat(logFetchBuffer.nextInLineFetch()).isNotNull();

        // Validate that the next fetch position has been updated to point to the record after our
        // last fetched record.
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(10L);

        // Now attempt to collect more records from the fetch buffer.
        bucketAndRecords = logFetchCollector.collectFetch(logFetchBuffer);
        assertThat(bucketAndRecords.size()).isEqualTo(0);
    }

    @Test
    void testCollectAfterUnassign() throws Exception {
        TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 1L, 1);
        TableBucket tb2 = new TableBucket(DATA1_TABLE_ID, 1L, 2);
        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        scanBuckets.put(tb1, 0L);
        scanBuckets.put(tb2, 0L);
        logScannerStatus.assignScanBuckets(scanBuckets);

        FetchLogResultForBucket resultForBucket1 =
                new FetchLogResultForBucket(tb1, genMemoryLogRecordsByObject(DATA1), 10L);
        FetchLogResultForBucket resultForBucket2 =
                new FetchLogResultForBucket(tb2, genMemoryLogRecordsByObject(DATA1), 10L);
        CompletedFetch completedFetch1 = makeCompletedFetch(tb1, resultForBucket1, 0L);
        CompletedFetch completedFetch2 = makeCompletedFetch(tb2, resultForBucket2, 0L);

        logFetchBuffer.add(completedFetch1);
        logFetchBuffer.add(completedFetch2);

        // unassign bucket 2
        logScannerStatus.unassignScanBuckets(Collections.singletonList(tb2));

        Map<TableBucket, List<ScanRecord>> bucketAndRecords =
                logFetchCollector.collectFetch(logFetchBuffer);
        // should only contain records for bucket 1
        assertThat(bucketAndRecords.keySet()).containsExactly(tb1);

        // collect again, should be empty
        bucketAndRecords = logFetchCollector.collectFetch(logFetchBuffer);
        assertThat(bucketAndRecords.size()).isEqualTo(0);
    }

    private DefaultCompletedFetch makeCompletedFetch(
            TableBucket tableBucket, FetchLogResultForBucket resultForBucket, long offset) {
        return new DefaultCompletedFetch(
                tableBucket, resultForBucket, readContext, logScannerStatus, true, offset);
    }
}
