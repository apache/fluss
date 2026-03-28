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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultCompletedFetch} zero-copy buffer lifecycle (issue #1184). */
class DefaultCompletedFetchBufferLifecycleTest {

    private final TableBucket tb0 = new TableBucket(1, 0);
    private final TableBucket tb1 = new TableBucket(1, 1);
    private LogScannerStatus logScannerStatus;
    private LogRecordReadContext readContext;

    @BeforeEach
    void setup() {
        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        scanBuckets.put(tb0, 0L);
        scanBuckets.put(tb1, 0L);
        logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(scanBuckets);
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
    void testBufferReleasedOnDrain() throws Exception {
        ByteBuf buf = Unpooled.buffer(64);
        buf.writeBytes(new byte[64]);

        buf.retain();
        DefaultCompletedFetch fetch = makeCompletedFetch(tb0, buf);
        buf.release(); // base reference
        assertThat(buf.refCnt()).isEqualTo(1);

        fetch.drain();
        assertThat(buf.refCnt()).isEqualTo(0);
    }

    @Test
    void testMultipleCompletedFetchShareBuffer() throws Exception {
        ByteBuf buf = Unpooled.buffer(64);
        buf.writeBytes(new byte[64]);

        buf.retain();
        DefaultCompletedFetch fetch1 = makeCompletedFetch(tb0, buf);
        buf.retain();
        DefaultCompletedFetch fetch2 = makeCompletedFetch(tb1, buf);
        buf.release(); // base reference
        assertThat(buf.refCnt()).isEqualTo(2);

        fetch1.drain();
        assertThat(buf.refCnt()).isEqualTo(1);

        fetch2.drain();
        assertThat(buf.refCnt()).isEqualTo(0);
    }

    @Test
    void testDrainIsIdempotent() throws Exception {
        ByteBuf buf = Unpooled.buffer(64);
        buf.writeBytes(new byte[64]);
        buf.retain();

        DefaultCompletedFetch fetch = makeCompletedFetch(tb0, buf);
        buf.release();

        fetch.drain();
        assertThat(buf.refCnt()).isEqualTo(0);
        fetch.drain(); // no-op, no exception
    }

    @Test
    void testNullBufferHandledGracefully() throws Exception {
        DefaultCompletedFetch fetch = makeCompletedFetch(tb0, null);
        fetch.drain();
    }

    @Test
    void testLogFetchBufferCloseReleasesBuffer() throws Exception {
        ByteBuf buf = Unpooled.buffer(64);
        buf.writeBytes(new byte[64]);

        buf.retain();
        DefaultCompletedFetch fetch = makeCompletedFetch(tb0, buf);
        buf.release();

        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            logFetchBuffer.add(fetch);
            assertThat(buf.refCnt()).isEqualTo(1);
        }
        assertThat(buf.refCnt()).isEqualTo(0);
    }

    @Test
    void testRetainAllUnsubscribeReleasesBuffer() throws Exception {
        ByteBuf buf = Unpooled.buffer(64);
        buf.writeBytes(new byte[64]);

        buf.retain();
        DefaultCompletedFetch fetch = makeCompletedFetch(tb0, buf);
        buf.release();

        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            logFetchBuffer.add(fetch);
            logFetchBuffer.retainAll(Collections.singleton(tb1));
            assertThat(buf.refCnt()).isEqualTo(0);
        }
    }

    private DefaultCompletedFetch makeCompletedFetch(TableBucket tableBucket, ByteBuf parsedByteBuf)
            throws Exception {
        return new DefaultCompletedFetch(
                tableBucket,
                new FetchLogResultForBucket(tableBucket, genMemoryLogRecordsByObject(DATA1), 10L),
                readContext,
                logScannerStatus,
                true,
                0L,
                parsedByteBuf);
    }
}
