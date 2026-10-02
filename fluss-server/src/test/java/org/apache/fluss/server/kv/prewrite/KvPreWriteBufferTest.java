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

import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.PreparedFlush;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer}. */
class KvPreWriteBufferTest {

    @Test
    void testIllegalLSN() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);
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
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);
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
        flushBuffer(buffer, 1);

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
        flushBuffer(buffer, elementCount + 1);

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
        flushBuffer(buffer, elementCount + 1);

        // check write buffer, entry count in the buffer should be 0
        assertThat(buffer.getAllKvEntries().size()).isEqualTo(0);
        assertThat(buffer.getKvEntryMap().size()).isEqualTo(0);

        // we can get nothing then
        assertThat(getValue(buffer, "key3")).isNull();
        assertThat(getValue(buffer, "key2")).isNull();
    }

    @Test
    void testTruncate() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);
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
    void testRowCount() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);
        int elementCount = 0;

        // put a series of kv entries
        for (int i = 0; i < 10; i++) {
            bufferInsert(buffer, "key" + i, "value" + i, elementCount++);
        }
        assertThat(flushBuffer(buffer, Long.MAX_VALUE)).isEqualTo(10);

        // delete some keys
        for (int i = 0; i < 5; i++) {
            bufferDelete(buffer, "key" + i, elementCount++);
        }
        assertThat(flushBuffer(buffer, Long.MAX_VALUE)).isEqualTo(-5);

        // put some keys again
        for (int i = 8; i < 9; i++) {
            bufferUpdate(buffer, "key" + i, "value" + i, elementCount++);
        }
        assertThat(flushBuffer(buffer, Long.MAX_VALUE)).isEqualTo(0);

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
        assertThat(flushBuffer(buffer, Long.MAX_VALUE)).isEqualTo(7);
    }

    @Test
    void testSplitPreparedFlushByRecordCount() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);
        // +key0(lsn 0), +key1(lsn 1), +key2(lsn 2), +key3(lsn 3), -key2(lsn 4)
        for (int i = 0; i < 4; i++) {
            bufferInsert(buffer, "key" + i, "value" + i, i);
            buffer.markWalBatchEnd(i + 1);
        }
        bufferDelete(buffer, "key2", 4);
        buffer.markWalBatchEnd(5);
        buffer.markWalBatchEnd(10);

        PreparedFlush preparedFlush = buffer.prepareFlush(10);
        List<PreparedFlush> segments = preparedFlush.split(0, 2);

        assertThat(segments).hasSize(3);
        // Each entry here is a complete WAL batch. The last segment includes the empty tail.
        assertThat(segments.get(0).entries()).hasSize(2);
        assertThat(segments.get(0).exclusiveUpToLogSequenceNumber()).isEqualTo(2);
        assertThat(segments.get(1).entries()).hasSize(2);
        assertThat(segments.get(1).exclusiveUpToLogSequenceNumber()).isEqualTo(4);
        assertThat(segments.get(2).entries()).hasSize(1);
        assertThat(segments.get(2).exclusiveUpToLogSequenceNumber()).isEqualTo(10);
        // Row count diffs are distributed per segment and sum up to the whole flush.
        assertThat(segments.get(0).rowCountDiff()).isEqualTo(2);
        assertThat(segments.get(1).rowCountDiff()).isEqualTo(2);
        assertThat(segments.get(2).rowCountDiff()).isEqualTo(-1);

        // Segments complete in order as list prefixes.
        assertThat(buffer.completeFlush(segments.get(0))).isEqualTo(2);
        assertThat(buffer.completeFlush(segments.get(1))).isEqualTo(2);
        assertThat(buffer.completeFlush(segments.get(2))).isEqualTo(-1);
        assertThat(buffer.getAllKvEntries()).isEmpty();
        assertThat(buffer.pendingFlushBytes()).isEqualTo(0);
    }

    @Test
    void testSplitPreparedFlushByByteSize() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);
        // Entry payload sizes are 6, 5, and 5 bytes.
        bufferInsert(buffer, "a", "12345", 0);
        buffer.markWalBatchEnd(1);
        bufferInsert(buffer, "b", "1234", 1);
        buffer.markWalBatchEnd(2);
        bufferInsert(buffer, "c", "1234", 2);
        buffer.markWalBatchEnd(3);

        PreparedFlush preparedFlush = buffer.prepareFlush(3);
        List<PreparedFlush> segments = preparedFlush.split(10, Integer.MAX_VALUE);

        // The first two complete batches cross the byte budget and close the first segment.
        assertThat(segments).hasSize(2);
        assertThat(segments.get(0).entries()).hasSize(2);
        assertThat(segments.get(0).exclusiveUpToLogSequenceNumber()).isEqualTo(2);
        assertThat(segments.get(0).rowCountDiff()).isEqualTo(2);
        assertThat(segments.get(1).entries()).hasSize(1);
        assertThat(segments.get(1).exclusiveUpToLogSequenceNumber()).isEqualTo(3);
        assertThat(segments.get(1).rowCountDiff()).isEqualTo(1);
    }

    @Test
    void testSplitPreparedFlushWithOversizedEntry() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);
        // The first entry is larger than the byte limit and must remain a non-empty singleton.
        bufferInsert(buffer, "a", "1234567890", 0);
        buffer.markWalBatchEnd(1);
        bufferInsert(buffer, "b", "123", 1);
        buffer.markWalBatchEnd(2);

        PreparedFlush preparedFlush = buffer.prepareFlush(2);
        List<PreparedFlush> segments = preparedFlush.split(10, Integer.MAX_VALUE);

        assertThat(segments).hasSize(2);
        assertThat(segments.get(0).entries()).hasSize(1);
        assertThat(segments.get(0).exclusiveUpToLogSequenceNumber()).isEqualTo(1);
        assertThat(segments.get(1).entries()).hasSize(1);
        assertThat(segments.get(1).exclusiveUpToLogSequenceNumber()).isEqualTo(2);
    }

    @Test
    void testSplitPreparedFlushUsesFirstReachedLimit() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);
        // Entry payload sizes are 2, 2, 11, and 5 bytes.
        bufferInsert(buffer, "a", "x", 0);
        buffer.markWalBatchEnd(1);
        bufferInsert(buffer, "b", "y", 1);
        buffer.markWalBatchEnd(2);
        bufferInsert(buffer, "c", "1234567890", 2);
        buffer.markWalBatchEnd(3);
        bufferInsert(buffer, "d", "1234", 3);
        buffer.markWalBatchEnd(4);

        PreparedFlush preparedFlush = buffer.prepareFlush(4);
        List<PreparedFlush> segments = preparedFlush.split(10, 2);

        // The record limit closes the first segment, then the byte limit closes the second.
        assertThat(segments).hasSize(3);
        assertThat(segments.get(0).entries()).hasSize(2);
        assertThat(segments.get(0).exclusiveUpToLogSequenceNumber()).isEqualTo(2);
        assertThat(segments.get(1).entries()).hasSize(1);
        assertThat(segments.get(1).exclusiveUpToLogSequenceNumber()).isEqualTo(3);
        assertThat(segments.get(2).entries()).hasSize(1);
        assertThat(segments.get(2).exclusiveUpToLogSequenceNumber()).isEqualTo(4);
    }

    @Test
    void testCompletePrefixSegmentsAndAbortRest() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);
        // Entry payload sizes are 6, 5, 5, and 6 bytes.
        bufferInsert(buffer, "a", "12345", 0);
        buffer.markWalBatchEnd(1);
        bufferInsert(buffer, "b", "1234", 1);
        buffer.markWalBatchEnd(2);
        bufferInsert(buffer, "c", "1234", 2);
        buffer.markWalBatchEnd(3);
        bufferInsert(buffer, "d", "12345", 3);
        buffer.markWalBatchEnd(4);

        PreparedFlush preparedFlush = buffer.prepareFlush(4);
        List<PreparedFlush> segments = preparedFlush.split(10, Integer.MAX_VALUE);
        assertThat(segments).hasSize(2);

        // Model a storage rejection after the first segment landed: complete the written prefix,
        // abort the rest.
        assertThat(buffer.completeFlush(segments.get(0))).isEqualTo(2);
        buffer.abortFlush(segments.get(1));

        // The completed entries are gone; the aborted ones are ACTIVE again and can be prepared
        // by the retry, which must cover exactly the remaining range.
        assertThat(buffer.pendingFlushBytes()).isEqualTo(11);
        assertThat(buffer.getAllKvEntries()).hasSize(2);
        PreparedFlush retry = buffer.prepareFlush(4);
        assertThat(retry.entries()).hasSize(2);
        assertThat(retry.entries().get(0).getLogSequenceNumber()).isEqualTo(2);
        assertThat(retry.rowCountDiff()).isEqualTo(2);
    }

    private static void bufferInsert(
            KvPreWriteBuffer kvPreWriteBuffer, String key, String value, int elementCount) {
        kvPreWriteBuffer.insert(toKey(key), value.getBytes(), elementCount);
    }

    private static void bufferUpdate(
            KvPreWriteBuffer kvPreWriteBuffer, String key, String value, int elementCount) {
        kvPreWriteBuffer.update(toKey(key), value.getBytes(), elementCount);
    }

    private static void bufferDelete(
            KvPreWriteBuffer kvPreWriteBuffer, String key, int elementCount) {
        kvPreWriteBuffer.delete(toKey(key), elementCount);
    }

    @Test
    void testPrepareAndCompleteFlush() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);

        bufferInsert(buffer, "key1", "value1", 1);
        bufferInsert(buffer, "key2", "value2", 2);
        bufferInsert(buffer, "key3", "value3", 3);

        PreparedFlush preparedFlush = buffer.prepareFlush(3);

        assertThat(preparedFlush.entries()).hasSize(2);
        assertThat(preparedFlush.rowCountDiff()).isEqualTo(2);
        assertThat(buffer.pendingFlushBytes()).isEqualTo(30);
        assertThat(getValue(buffer, "key1")).isEqualTo("value1");
        assertThat(getValue(buffer, "key2")).isEqualTo("value2");

        assertThat(buffer.completeFlush(preparedFlush)).isEqualTo(2);

        assertThat(buffer.pendingFlushBytes()).isEqualTo(10);
        assertThat(buffer.getAllKvEntries()).hasSize(1);
        assertThat(getValue(buffer, "key1")).isNull();
        assertThat(getValue(buffer, "key2")).isNull();
        assertThat(getValue(buffer, "key3")).isEqualTo("value3");
    }

    @Test
    void testAbortPreparedFlush() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);

        bufferInsert(buffer, "key1", "value1", 1);
        bufferDelete(buffer, "key2", 2);

        PreparedFlush preparedFlush = buffer.prepareFlush(3);
        assertThat(buffer.pendingFlushBytes()).isEqualTo(14);

        buffer.abortFlush(preparedFlush);

        assertThat(buffer.pendingFlushBytes()).isEqualTo(14);
        assertThat(getValue(buffer, "key1")).isEqualTo("value1");
        assertThat(getValue(buffer, "key2")).isNull();

        PreparedFlush nextPreparedFlush = buffer.prepareFlush(3);
        assertThat(nextPreparedFlush.entries()).hasSize(2);
    }

    @Test
    void testCannotTruncatePreparedFlush() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);

        bufferInsert(buffer, "key1", "value1", 1);
        bufferInsert(buffer, "key2", "value2", 2);
        buffer.prepareFlush(3);

        assertThatThrownBy(() -> buffer.truncateTo(1, TruncateReason.ERROR))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot truncate prepared pre-write entry.");
    }

    @Test
    void testPendingFlushBytesTracking() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);

        // Initially zero
        assertThat(buffer.pendingFlushBytes()).isEqualTo(0);

        // Insert key1=value1 (4 + 6 = 10 bytes)
        bufferInsert(buffer, "key1", "value1", 1);
        assertThat(buffer.pendingFlushBytes()).isEqualTo(10);

        // Insert key2=value22 (4 + 7 = 11 bytes)
        bufferInsert(buffer, "key2", "value22", 2);
        assertThat(buffer.pendingFlushBytes()).isEqualTo(21);

        // Delete key3: key bytes only (4 + 0 = 4 bytes for delete with null value)
        bufferDelete(buffer, "key3", 3);
        assertThat(buffer.pendingFlushBytes()).isEqualTo(25);

        // Flush entries with lsn < 2 (only key1): decreases by 10
        flushBuffer(buffer, 2);
        assertThat(buffer.pendingFlushBytes()).isEqualTo(15);

        // TruncateTo(3) removes entries with lsn >= 3 (key3 delete): decreases by 4
        buffer.truncateTo(3, TruncateReason.ERROR);
        assertThat(buffer.pendingFlushBytes()).isEqualTo(11);

        // Flush remaining (key2): decreases by 11
        flushBuffer(buffer, Long.MAX_VALUE);
        assertThat(buffer.pendingFlushBytes()).isEqualTo(0);
    }

    @Test
    void testCompleteFlushDetachesFlushedEntriesFromPreviousChain() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);
        KvPreWriteBuffer.Key key = toKey("k");

        buffer.insert(key, "v1".getBytes(), 1);
        buffer.insert(key, "v2".getBytes(), 2);
        buffer.insert(key, "v3".getBytes(), 3);
        KvPreWriteBuffer.KvEntry v3 = buffer.getKvEntryMap().get(key);
        buffer.insert(key, "v4".getBytes(), 4);
        KvPreWriteBuffer.KvEntry v4 = buffer.getKvEntryMap().get(key);

        // flush covers v1 and v2; v3 and v4 stay buffered
        flushBuffer(buffer, 3);

        // the buffered chain is cut at the flushed boundary: v3 no longer references
        // the flushed v2, while the references to the still-buffered v3 are kept for rollback
        assertThat(v3.getPreviousEntry()).isNull();
        assertThat(v4.getPreviousEntry()).isSameAs(v3);
        assertThat(v3.getNextEntry()).isSameAs(v4);

        // read semantics are unchanged and only v3/v4 stay in the byte accounting
        assertThat(getValue(buffer, "k")).isEqualTo("v4");
        assertThat(buffer.pendingFlushBytes()).isEqualTo(6);

        // across further flush cycles the chain never grows back: after flushing v3~v5,
        // the remaining v6 references no flushed version any more
        buffer.insert(key, "v5".getBytes(), 5);
        buffer.insert(key, "v6".getBytes(), 6);
        KvPreWriteBuffer.KvEntry v6 = buffer.getKvEntryMap().get(key);
        flushBuffer(buffer, 6);
        assertThat(v6.getPreviousEntry()).isNull();
        assertThat(getValue(buffer, "k")).isEqualTo("v6");
    }

    @Test
    void testTruncateRollbackSemanticsAfterFlushDetachment() {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(TestingMetricGroups.TABLET_SERVER_METRICS);
        KvPreWriteBuffer.Key key = toKey("k");

        // v1@1, v2@2, v3@3; flush covers v1 and v2, v3 stays buffered
        buffer.insert(key, "v1".getBytes(), 1);
        buffer.insert(key, "v2".getBytes(), 2);
        buffer.insert(key, "v3".getBytes(), 3);
        flushBuffer(buffer, 3);

        // truncating v3 rolls back to the newest version below the truncation point; all
        // older versions are flushed, so the rollback target is the kv storage and the key
        // disappears from the buffer - the same behavior as before the chain detachment
        buffer.truncateTo(1, TruncateReason.ERROR);
        assertThat(getValue(buffer, "k")).isNull();
        assertThat(buffer.getKvEntryMap()).doesNotContainKey(key);
        assertThat(buffer.getAllKvEntries()).isEmpty();

        // rollback to a still-buffered previous version must keep working: after flushing v4,
        // truncating v6 rolls the map back to the buffered v5
        buffer.insert(key, "v4".getBytes(), 4);
        buffer.insert(key, "v5".getBytes(), 5);
        flushBuffer(buffer, 5);
        buffer.insert(key, "v6".getBytes(), 6);
        buffer.truncateTo(6, TruncateReason.ERROR);
        assertThat(getValue(buffer, "k")).isEqualTo("v5");
        // the retained predecessor no longer links forward to the truncated entry
        assertThat(buffer.getKvEntryMap().get(key).getNextEntry()).isNull();
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

    /**
     * Flushes the buffer using the production two-phase path: prepareFlush + completeFlush.
     *
     * @return the row count difference reported by completeFlush.
     */
    private static int flushBuffer(KvPreWriteBuffer buffer, long exclusiveUpToLsn) {
        PreparedFlush prepared = buffer.prepareFlush(exclusiveUpToLsn);
        return buffer.completeFlush(prepared);
    }
}
