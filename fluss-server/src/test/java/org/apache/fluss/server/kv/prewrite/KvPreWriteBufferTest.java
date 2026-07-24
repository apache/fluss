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

package org.apache.fluss.server.kv.prewrite;

import org.apache.fluss.exception.InsufficientKvPreWriteBufferException;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.server.kv.KvBatchWriter;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer}. */
class KvPreWriteBufferTest {

    @Test
    void testIllegalLSN() {
        KvPreWriteBuffer buffer = createBuffer(new NopKvBatchWriter());
        bufferInsert(buffer, "key1", "value1", 1);
        bufferDelete(buffer, "key1", 3);

        assertThatThrownBy(() -> bufferInsert(buffer, "key2", "value2", 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The log sequence number must be non-decreasing. The current "
                                + "log sequence number is 3, but the new log sequence number is 2");

        assertThatThrownBy(() -> bufferDelete(buffer, "key2", 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The log sequence number must be non-decreasing. The current "
                                + "log sequence number is 3, but the new log sequence number is 1");
    }

    @Test
    void testWriteAndFlush() throws Exception {
        KvPreWriteBuffer buffer = createBuffer(new NopKvBatchWriter());
        int elementCount = 0;

        // put a series of kv entries
        for (int i = 0; i < 3; i++) {
            bufferInsert(buffer, "key" + i, "value" + i, elementCount++);
        }
        // check the key and value;
        for (int i = 0; i < 3; i++) {
            String value = getValue(buffer, "key" + i);
            assertThat(value).isEqualTo("value" + i);
        }

        // then delete key2
        bufferDelete(buffer, "key2", elementCount++);
        // can't get key2 then
        assertThat(getValue(buffer, "key2")).isNull();
        // then check the other keys
        for (int i = 0; i < 2; i++) {
            String value = getValue(buffer, "key" + i);
            assertThat(value).isEqualTo("value" + i);
        }

        // +key0, +key1, +key2, -key2
        // then flush up to offset 1;
        buffer.flush(1);

        // check the all entries in the buffer is 3
        assertThat(buffer.getAllKvEntries().size()).isEqualTo(3);
        // the entry count in the map is 2, for +key1, -key2
        assertThat(buffer.getKvEntryMap().size()).isEqualTo(2);

        // then we can't get key0,
        assertThat(getValue(buffer, "key0")).isNull();
        // we can get key1
        assertThat(getValue(buffer, "key1")).isEqualTo("value1");
        // check key2 is null since we delete it
        assertThat(getValue(buffer, "key2")).isNull();

        // put key2 again
        bufferInsert(buffer, "key2", "value21", elementCount++);
        // we can get key2
        assertThat(getValue(buffer, "key2")).isEqualTo("value21");

        // flush all;
        buffer.flush(elementCount + 1);

        // check write buffer, entry count in the buffer should be 0
        assertThat(buffer.getAllKvEntries().size()).isEqualTo(0);
        assertThat(buffer.getKvEntryMap().size()).isEqualTo(0);

        // get can get nothing
        for (int i = 0; i < 3; i++) {
            assertThat(buffer.get(toKey("key" + i))).isNull();
        }

        // put two key3;
        bufferInsert(buffer, "key3", "value31", elementCount++);
        bufferInsert(buffer, "key3", "value32", elementCount++);
        bufferInsert(buffer, "key2", "value22", elementCount++);
        // check get key3 get the latest value
        assertThat(getValue(buffer, "key3")).isEqualTo("value32");
        // check get key2
        assertThat(getValue(buffer, "key2")).isEqualTo("value22");

        // flush all
        buffer.flush(elementCount + 1);

        // check write buffer, entry count in the buffer should be 0
        assertThat(buffer.getAllKvEntries().size()).isEqualTo(0);
        assertThat(buffer.getKvEntryMap().size()).isEqualTo(0);

        // we can get nothing then
        assertThat(getValue(buffer, "key3")).isNull();
        assertThat(getValue(buffer, "key2")).isNull();

        buffer.close();
    }

    @Test
    void testTruncate() {
        KvPreWriteBuffer buffer = createBuffer(new NopKvBatchWriter());
        int elementCount = 0;

        // put a series of kv entries
        for (int i = 0; i < 10; i++) {
            bufferInsert(buffer, "key" + i, "value" + i, elementCount++);
        }
        // check the key and value;
        for (int i = 0; i < 10; i++) {
            String value = getValue(buffer, "key" + i);
            assertThat(value).isEqualTo("value" + i);
        }
        assertThat(buffer.getMaxLSN()).isEqualTo(elementCount - 1);

        // truncate to 5.
        buffer.truncateTo(5, TruncateReason.ERROR);
        assertThat(buffer.getMaxLSN()).isEqualTo(4);
        assertThat(buffer.getAllKvEntries().size()).isEqualTo(5);
        for (int i = 0; i < 5; i++) {
            String value = getValue(buffer, "key" + i);
            assertThat(value).isEqualTo("value" + i);
        }
        assertThat(getValue(buffer, "key6")).isNull();

        // add delete records.
        elementCount = 5;
        bufferDelete(buffer, "key4", elementCount++);
        bufferDelete(buffer, "key3", elementCount++);
        assertThat(getValue(buffer, "key3")).isNull();

        // add update records
        bufferInsert(buffer, "key2", "value2-1", elementCount++);
        bufferInsert(buffer, "key1", "value1-1", elementCount++);
        assertThat(getValue(buffer, "key1")).isEqualTo("value1-1");
        assertThat(buffer.getMaxLSN()).isEqualTo(elementCount - 1);
        buffer.truncateTo(5, TruncateReason.ERROR);
        assertThat(buffer.getMaxLSN()).isEqualTo(4);
        assertThat(buffer.getAllKvEntries().size()).isEqualTo(5);
        // to delete records and update records operation will be truncate.
        for (int i = 0; i < 5; i++) {
            String value = getValue(buffer, "key" + i);
            assertThat(value).isEqualTo("value" + i);
        }

        // truncate to zero
        buffer.truncateTo(0, TruncateReason.ERROR);
        assertThat(buffer.getMaxLSN()).isEqualTo(-1);
        assertThat(buffer.getAllKvEntries().size()).isEqualTo(0);
        assertThat(buffer.getKvEntryMap().size()).isEqualTo(0);
    }

    @Test
    void testRowCount() throws IOException {
        KvPreWriteBuffer buffer = createBuffer(new NopKvBatchWriter());
        int elementCount = 0;

        // put a series of kv entries
        for (int i = 0; i < 10; i++) {
            bufferInsert(buffer, "key" + i, "value" + i, elementCount++);
        }
        assertThat(buffer.flush(Long.MAX_VALUE)).isEqualTo(10);

        // delete some keys
        for (int i = 0; i < 5; i++) {
            bufferDelete(buffer, "key" + i, elementCount++);
        }
        assertThat(buffer.flush(Long.MAX_VALUE)).isEqualTo(-5);

        // put some keys again
        for (int i = 8; i < 9; i++) {
            bufferUpdate(buffer, "key" + i, "value" + i, elementCount++);
        }
        assertThat(buffer.flush(Long.MAX_VALUE)).isEqualTo(0);

        // put some keys again
        for (int i = 10; i < 20; i++) {
            bufferInsert(buffer, "key" + i, "value" + i, elementCount++);
        }
        for (int i = 10; i < 13; i++) {
            bufferDelete(buffer, "key" + i, elementCount++);
        }
        // restore to here, so the row count should be 10 - 3 = 7
        int checkpoint = elementCount;
        for (int i = 30; i < 35; i++) {
            bufferInsert(buffer, "key" + i, "value" + i, elementCount++);
        }
        for (int i = 30; i < 35; i++) {
            bufferUpdate(buffer, "key" + i, "value" + i, elementCount++);
        }

        // truncate to 5
        buffer.truncateTo(checkpoint, TruncateReason.ERROR);
        assertThat(buffer.flush(Long.MAX_VALUE)).isEqualTo(7);
    }

    @Test
    void testMemoryAccountingForMultipleVersions() throws Exception {
        KvPreWriteBufferMemoryManager memoryManager =
                new KvPreWriteBufferMemoryManager(10_000, 8_000);
        KvPreWriteBuffer buffer =
                new KvPreWriteBuffer(
                        new NopKvBatchWriter(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        memoryManager);

        buffer.insert(toKey("key"), "value-1".getBytes(), 0);
        long firstEntryBytes = memoryManager.usedBytes();
        buffer.update(toKey("key"), "value-2".getBytes(), 1);
        long secondEntryBytes = memoryManager.usedBytes() - firstEntryBytes;
        buffer.delete(toKey("key"), 2);
        long thirdEntryBytes = memoryManager.usedBytes() - firstEntryBytes - secondEntryBytes;
        assertThat(memoryManager.usedBytes())
                .isEqualTo(firstEntryBytes + secondEntryBytes + thirdEntryBytes);

        buffer.flush(1);
        assertThat(memoryManager.usedBytes()).isEqualTo(secondEntryBytes + thirdEntryBytes);
        assertThat(buffer.getAllKvEntries()).hasSize(2);

        buffer.truncateTo(2, TruncateReason.ERROR);
        assertThat(memoryManager.usedBytes()).isEqualTo(secondEntryBytes);
        assertThat(getValue(buffer, "key")).isEqualTo("value-2");

        buffer.flush(Long.MAX_VALUE);
        assertThat(memoryManager.usedBytes()).isZero();
        assertThat(buffer.getAllKvEntries()).isEmpty();

        buffer.close();
        assertThat(memoryManager.usedBytes()).isZero();
    }

    @Test
    void testRejectedReservationDoesNotMutateBuffer() {
        KvPreWriteBufferMemoryManager memoryManager = new KvPreWriteBufferMemoryManager(200, 160);
        KvPreWriteBuffer buffer =
                new KvPreWriteBuffer(
                        new NopKvBatchWriter(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        memoryManager);
        buffer.insert(toKey("key"), "value".getBytes(), 0);
        long retainedBytes = memoryManager.usedBytes();

        assertThatThrownBy(() -> buffer.insert(toKey("key-2"), "value-2".getBytes(), 1))
                .isInstanceOf(InsufficientKvPreWriteBufferException.class);
        assertThat(memoryManager.usedBytes()).isEqualTo(retainedBytes);
        assertThat(buffer.getAllKvEntries()).hasSize(1);
        assertThat(buffer.getMaxLSN()).isZero();
        assertThat(buffer.get(toKey("key-2"))).isNull();

        buffer.truncateTo(0, TruncateReason.ERROR);
        assertThat(memoryManager.usedBytes()).isZero();
        assertThat(memoryManager.isUnderPressure()).isFalse();
    }

    @Test
    void testEntryLargerThanHighWatermarkIsNotRetriable() {
        KvPreWriteBufferMemoryManager memoryManager = new KvPreWriteBufferMemoryManager(100, 80);
        KvPreWriteBuffer buffer =
                new KvPreWriteBuffer(
                        new NopKvBatchWriter(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        memoryManager);

        assertThatThrownBy(() -> buffer.insert(toKey("key"), "value".getBytes(), 0))
                .isInstanceOf(RecordTooLargeException.class);
        assertThat(memoryManager.usedBytes()).isZero();
        assertThat(buffer.getAllKvEntries()).isEmpty();
        assertThat(buffer.getKvEntryMap()).isEmpty();
    }

    @Test
    void testMemoryQuotaIsSharedAcrossBuffers() throws Exception {
        KvPreWriteBufferMemoryManager memoryManager = new KvPreWriteBufferMemoryManager(200, 160);
        KvPreWriteBuffer firstBuffer =
                new KvPreWriteBuffer(
                        new NopKvBatchWriter(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        memoryManager);
        KvPreWriteBuffer secondBuffer =
                new KvPreWriteBuffer(
                        new NopKvBatchWriter(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        memoryManager);

        firstBuffer.insert(toKey("key"), "value".getBytes(), 0);
        assertThatThrownBy(() -> secondBuffer.insert(toKey("key"), "value".getBytes(), 0))
                .isInstanceOf(InsufficientKvPreWriteBufferException.class);

        firstBuffer.close();
        secondBuffer.insert(toKey("key"), "value".getBytes(), 0);
        assertThat(secondBuffer.getAllKvEntries()).hasSize(1);
        secondBuffer.close();
        assertThat(memoryManager.usedBytes()).isZero();
    }

    @Test
    void testCloseReleasesMemory() throws Exception {
        KvPreWriteBufferMemoryManager memoryManager =
                new KvPreWriteBufferMemoryManager(10_000, 8_000);
        KvPreWriteBuffer buffer =
                new KvPreWriteBuffer(
                        new NopKvBatchWriter(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        memoryManager);
        buffer.insert(toKey("key"), "value".getBytes(), 0);

        buffer.close();

        assertThat(memoryManager.usedBytes()).isZero();
    }

    private static void bufferInsert(
            KvPreWriteBuffer kvPreWriteBuffer, String key, String value, int elementCount) {
        kvPreWriteBuffer.insert(toKey(key), value.getBytes(), elementCount);
    }

    private static KvPreWriteBuffer createBuffer(KvBatchWriter kvBatchWriter) {
        return new KvPreWriteBuffer(
                kvBatchWriter,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                new KvPreWriteBufferMemoryManager(10_000, 8_000));
    }

    private static void bufferUpdate(
            KvPreWriteBuffer kvPreWriteBuffer, String key, String value, int elementCount) {
        kvPreWriteBuffer.update(toKey(key), value.getBytes(), elementCount);
    }

    private static void bufferDelete(
            KvPreWriteBuffer kvPreWriteBuffer, String key, int elementCount) {
        kvPreWriteBuffer.delete(toKey(key), elementCount);
    }

    private static String getValue(KvPreWriteBuffer preWriteBuffer, String keyStr) {
        KvPreWriteBuffer.Key key = toKey(keyStr);
        KvPreWriteBuffer.Value value = preWriteBuffer.get(key);
        if (value != null && value.get() != null) {
            byte[] bytes = value.get();
            return bytes != null ? new String(bytes) : null;
        } else {
            return null;
        }
    }

    private static KvPreWriteBuffer.Key toKey(String str) {
        return KvPreWriteBuffer.Key.of(str.getBytes());
    }

    /** A {@link KvBatchWriter} for test purpose without doing anything. */
    private static class NopKvBatchWriter implements KvBatchWriter {

        @Override
        public void put(@Nonnull byte[] key, @Nonnull byte[] value) {
            // do nothing
        }

        @Override
        public void delete(@Nonnull byte[] key) {
            // do nothing
        }

        @Override
        public void flush() {
            // do nothing
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
