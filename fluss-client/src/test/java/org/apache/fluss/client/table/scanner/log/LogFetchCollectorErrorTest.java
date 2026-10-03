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

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests error delivery alongside progress and completion for both collector formats. */
class LogFetchCollectorErrorTest {
    private final TableBucket finishedBucket = new TableBucket(DATA1_TABLE_ID, 0);
    private final TableBucket errorBucket = new TableBucket(DATA1_TABLE_ID, 1);
    private final LogScannerStatus status = new LogScannerStatus();
    private final LogFetchBuffer buffer = new LogFetchBuffer();
    private final LogRecordReadContext readContext =
            LogRecordReadContext.createArrowReadContext(
                    DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER);

    @AfterEach
    void close() throws Exception {
        buffer.close();
        readContext.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAuthorizationErrorDoesNotTrapPendingCompletion(boolean arrow) {
        AbstractLogFetchCollector<?, ?> collector = collector(arrow);
        status.assignScanBucket(finishedBucket, 0L, 0L);
        CompletedFetch failed = addError(Errors.AUTHORIZATION_EXCEPTION);

        assertThatThrownBy(() -> collector.collectFetch(buffer))
                .isInstanceOf(AuthorizationException.class);
        assertThat(buffer.peek()).isNull();
        assertThat(failed.isConsumed()).isTrue();
        assertThat(status.hasPendingFinishedBuckets()).isTrue();
        assertThat(collectProgressOnly(collector).finishedBuckets())
                .containsExactly(finishedBucket);
        assertThat(collectProgressOnly(collector).hasProgress()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCompletionDefersFetchErrorUntilNextPoll(boolean arrow) {
        AbstractLogFetchCollector<?, ?> collector = collector(arrow);
        status.assignScanBucket(finishedBucket, 0L, 0L);
        CompletedFetch failed = addError(Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION);

        ScanRecords result = collectProgressOnly(collector);
        assertThat(result.finishedBuckets()).containsExactly(finishedBucket);
        assertThat(result.isEmpty()).isTrue();
        assertThat(buffer.peek()).isSameAs(failed);
        assertThat(failed.isConsumed()).isFalse();
        assertThatThrownBy(() -> collector.collectFetch(buffer)).isInstanceOf(FetchException.class);
        assertThat(failed.isConsumed()).isTrue();
        assertThat(buffer.peek()).isNull();
        assertThat(collectProgressOnly(collector).hasProgress()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testProgressDefersFetchErrorUntilNextPoll(boolean arrow) {
        AbstractLogFetchCollector<?, ?> collector = collector(arrow);
        status.assignScanBucket(finishedBucket, 0L, LogScanner.NO_STOPPING_OFFSET);
        buffer.add(makeFetch(FetchLogResultForBucket.empty(finishedBucket, 10L, 10L)));
        CompletedFetch failed = addError(Errors.CORRUPT_MESSAGE);

        ScanRecords result = collectProgressOnly(collector);
        assertThat(result.consumedUpToOffset(finishedBucket)).isEqualTo(10L);
        assertThat(result.finishedBuckets()).isEmpty();
        assertThat(result.isEmpty()).isTrue();
        assertThat(buffer.peek()).isSameAs(failed);
        assertThatThrownBy(() -> collector.collectFetch(buffer)).isInstanceOf(FetchException.class);
        assertThat(failed.isConsumed()).isTrue();
        assertThat(buffer.peek()).isNull();
        assertThat(collectProgressOnly(collector).hasProgress()).isFalse();
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void testRecordErrorDefersForProgressOrCompletion(boolean arrow, boolean completion) {
        AbstractLogFetchCollector<?, ?> collector = collector(arrow);
        if (completion) {
            status.assignScanBucket(finishedBucket, 0L, 0L);
        } else {
            status.assignScanBucket(finishedBucket, 0L, LogScanner.NO_STOPPING_OFFSET);
            buffer.add(makeFetch(FetchLogResultForBucket.empty(finishedBucket, 10L, 10L)));
        }
        status.assignScanBucket(errorBucket, 0L, LogScanner.NO_STOPPING_OFFSET);
        FetchException failure = new FetchException("record decoding failed");
        CompletedFetch failed =
                new DefaultCompletedFetch(
                        errorBucket,
                        DATA1_TABLE_PATH,
                        FetchLogResultForBucket.empty(errorBucket, 10L, -1L),
                        readContext,
                        status,
                        true,
                        0L,
                        null) {
                    @Override
                    public List<ScanRecord> fetchRecords(int maxRecords) {
                        throw failure;
                    }

                    @Override
                    List<ArrowBatchData> fetchArrowBatches(int maxRecords) {
                        throw failure;
                    }
                };
        buffer.add(failed);
        CompletedFetch queued = makeFetch(FetchLogResultForBucket.empty(errorBucket, 10L, -1L));
        buffer.add(queued);

        ScanRecords result = collectProgressOnly(collector);
        assertThat(result.hasProgress()).isTrue();
        assertThat(result.isEmpty()).isTrue();
        if (completion) {
            assertThat(result.finishedBuckets()).containsExactly(finishedBucket);
        } else {
            assertThat(result.consumedUpToOffset(finishedBucket)).isEqualTo(10L);
        }
        assertThat(buffer.nextInLineFetch()).isSameAs(failed);
        assertThat(buffer.peek()).isSameAs(queued);
        assertThatThrownBy(() -> collector.collectFetch(buffer)).isSameAs(failure);
        // Record errors stay attached to the in-flight fetch; do not dequeue another response.
        assertThat(buffer.nextInLineFetch()).isSameAs(failed);
        assertThat(buffer.peek()).isSameAs(queued);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAuthorizationErrorIsDeferredAfterAccumulatedRecords(boolean arrow) throws Exception {
        AbstractLogFetchCollector<?, ?> collector = collector(arrow);

        long stoppingOffset = DATA1.size();
        status.assignScanBucket(finishedBucket, 0L, stoppingOffset);

        // Bucket A: consuming these records reaches the bounded stopping offset.
        buffer.add(
                makeFetch(
                        FetchLogResultForBucket.records(
                                finishedBucket, data1Records(), stoppingOffset, -1L, -1L)));

        // Bucket B: processed after bucket A and fails with a non-FetchException.
        CompletedFetch failed = addError(Errors.AUTHORIZATION_EXCEPTION);

        long initialMemory = readContext.getBufferAllocator().getAllocatedMemory();

        // The authorization error must not discard records that were already consumed
        // during this poll. Deliver bucket A's records and completion first.
        if (arrow) {
            try (ArrowScanRecords result = (ArrowScanRecords) collector.collectFetch(buffer)) {
                assertThat(result.count()).isEqualTo(DATA1.size());
                assertThat(result.records(finishedBucket)).isNotEmpty();
                assertThat(result.consumedUpToOffset(finishedBucket)).isEqualTo(stoppingOffset);
                assertThat(result.finishedBuckets()).containsExactly(finishedBucket);
                assertThat(result.hasProgress()).isTrue();

                // The failed fetch must remain queued for the next poll.
                assertThat(buffer.peek()).isSameAs(failed);
                assertThat(failed.isConsumed()).isFalse();
            }

            // The successfully delivered Arrow result owns its buffers. Closing that
            // result must release them normally.
            assertThat(readContext.getBufferAllocator().getAllocatedMemory())
                    .isEqualTo(initialMemory);
        } else {
            ScanRecords result = (ScanRecords) collector.collectFetch(buffer);

            assertThat(result.count()).isEqualTo(DATA1.size());
            assertThat(result.records(finishedBucket)).hasSize(DATA1.size());
            assertThat(result.consumedUpToOffset(finishedBucket)).isEqualTo(stoppingOffset);
            assertThat(result.finishedBuckets()).containsExactly(finishedBucket);
            assertThat(result.hasProgress()).isTrue();

            // The failed fetch must remain queued for the next poll.
            assertThat(buffer.peek()).isSameAs(failed);
            assertThat(failed.isConsumed()).isFalse();
        }

        // Completion was delivered together with bucket A's final records, so it must
        // no longer remain pending.
        assertThat(status.hasPendingFinishedBuckets()).isFalse();

        // With no newly accumulated result in this poll, the authorization error is
        // now delivered immediately.
        assertThatThrownBy(() -> collector.collectFetch(buffer))
                .isInstanceOf(AuthorizationException.class);

        assertThat(failed.isConsumed()).isTrue();
        assertThat(buffer.peek()).isNull();

        // Neither records, progress, nor completion may be repeated.
        assertThat(collectProgressOnly(collector).hasProgress()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAuthorizationErrorIsDeferredAfterBoundedProgress(boolean arrow) {
        AbstractLogFetchCollector<?, ?> collector = collector(arrow);

        status.assignScanBucket(finishedBucket, 0L, 20L);

        // No records, but bounded scanner progress advances from 0 to 10.
        buffer.add(makeFetch(FetchLogResultForBucket.empty(finishedBucket, 20L, 10L)));

        CompletedFetch failed = addError(Errors.AUTHORIZATION_EXCEPTION);

        ScanRecords first = collectProgressOnly(collector);

        assertThat(first.isEmpty()).isTrue();
        assertThat(first.hasProgress()).isTrue();
        assertThat(first.consumedUpToOffset(finishedBucket)).isEqualTo(10L);
        assertThat(first.finishedBuckets()).isEmpty();

        assertThat(buffer.peek()).isSameAs(failed);
        assertThat(failed.isConsumed()).isFalse();

        assertThatThrownBy(() -> collector.collectFetch(buffer))
                .isInstanceOf(AuthorizationException.class);

        assertThat(failed.isConsumed()).isTrue();
        assertThat(buffer.peek()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAuthorizationErrorRemainsImmediateAfterUnboundedRecords(boolean arrow)
            throws Exception {
        AbstractLogFetchCollector<?, ?> collector = collector(arrow);
        status.assignScanBucket(finishedBucket, 0L, LogScanner.NO_STOPPING_OFFSET);

        buffer.add(
                makeFetch(
                        FetchLogResultForBucket.records(
                                finishedBucket, data1Records(), DATA1.size(), -1L, -1L)));

        CompletedFetch failed = addError(Errors.AUTHORIZATION_EXCEPTION);

        long initialMemory = readContext.getBufferAllocator().getAllocatedMemory();

        // Legacy unbounded behavior: the authorization error is propagated
        // immediately even though records were already accumulated.
        assertThatThrownBy(() -> collector.collectFetch(buffer))
                .isInstanceOf(AuthorizationException.class);

        // Scanner progress from A is preserved even though A's records were not
        // returned from this poll.
        assertThat(status.getBucketOffset(finishedBucket)).isEqualTo(DATA1.size());

        if (arrow) {
            // Accumulated Arrow records must be released when the poll fails.
            assertThat(readContext.getBufferAllocator().getAllocatedMemory())
                    .isEqualTo(initialMemory);
        }

        // Preserve the parent queue semantics: because this poll had already
        // accumulated a result, the zero-byte authorization error remains queued.
        assertThat(failed.isConsumed()).isFalse();
        assertThat(buffer.peek()).isSameAs(failed);

        // With no accumulated result in the next poll, the same error is propagated
        // again and is now removed from the queue.
        assertThatThrownBy(() -> collector.collectFetch(buffer))
                .isInstanceOf(AuthorizationException.class);

        assertThat(failed.isConsumed()).isTrue();
        assertThat(buffer.peek()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOffsetOutOfRangeReportsRequestedOffset(boolean arrow) {
        AbstractLogFetchCollector<?, ?> collector = collector(arrow);
        long requestedOffset = 123L;

        status.assignScanBucket(errorBucket, requestedOffset, LogScanner.NO_STOPPING_OFFSET);

        CompletedFetch failed =
                makeFetch(
                        FetchLogResultForBucket.error(
                                errorBucket,
                                new ApiError(Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION, "failure")),
                        requestedOffset);

        buffer.add(failed);

        assertThatThrownBy(() -> collector.collectFetch(buffer))
                .isInstanceOf(FetchException.class)
                .hasMessageContaining("fetching offset 123");
    }

    private AbstractLogFetchCollector<?, ?> collector(boolean arrow) {
        TestingMetadataUpdater metadata =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));
        return arrow
                ? new ArrowLogFetchCollector(status, new Configuration(), metadata)
                : new LogFetchCollector(status, new Configuration(), metadata);
    }

    private CompletedFetch addError(Errors error) {
        status.assignScanBucket(errorBucket, 0L, LogScanner.NO_STOPPING_OFFSET);
        CompletedFetch fetch =
                makeFetch(
                        FetchLogResultForBucket.error(errorBucket, new ApiError(error, "failure")));
        buffer.add(fetch);
        return fetch;
    }

    private CompletedFetch makeFetch(FetchLogResultForBucket result) {
        return makeFetch(result, 0L);
    }

    private CompletedFetch makeFetch(FetchLogResultForBucket result, long requestedOffset) {
        return new DefaultCompletedFetch(
                result.getTableBucket(),
                DATA1_TABLE_PATH,
                result,
                readContext,
                status,
                true,
                requestedOffset,
                null);
    }

    private ScanRecords collectProgressOnly(AbstractLogFetchCollector<?, ?> collector) {
        Object result = collector.collectFetch(buffer);
        if (result instanceof ScanRecords) {
            return (ScanRecords) result;
        }
        try (ArrowScanRecords records = (ArrowScanRecords) result) {
            assertThat(records.count()).isZero();
            Map<TableBucket, Long> offsets = new HashMap<>();
            for (TableBucket bucket : records.buckets()) {
                Long offset = records.consumedUpToOffset(bucket);
                if (offset != null) {
                    offsets.put(bucket, offset);
                }
            }
            return new ScanRecords(Collections.emptyMap(), offsets, records.finishedBuckets());
        }
    }

    private MemoryLogRecords data1Records() throws Exception {
        return createBasicMemoryLogRecords(
                DATA1_ROW_TYPE,
                DEFAULT_SCHEMA_ID,
                0L,
                0L,
                LogRecordBatch.CURRENT_LOG_MAGIC_VALUE,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                Collections.nCopies(DATA1.size(), ChangeType.APPEND_ONLY),
                DATA1,
                LogFormat.ARROW,
                DEFAULT_COMPRESSION,
                true);
    }
}
