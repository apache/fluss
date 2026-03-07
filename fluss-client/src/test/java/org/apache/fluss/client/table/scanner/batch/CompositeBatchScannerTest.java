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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompositeBatchScanner}. */
class CompositeBatchScannerTest {

    private static final Duration TIMEOUT = Duration.ofMillis(10);

    // -------------------------------------------------------------------------
    // No-limit tests
    // -------------------------------------------------------------------------

    @Test
    void testPollBatchWithNoLimit() throws IOException {
        // Three scanners each holding rows [0], [1], [2].
        // CompositeBatchScanner should round-robin and eventually return all rows.
        StubBatchScanner s1 = scanner(0);
        StubBatchScanner s2 = scanner(1);
        StubBatchScanner s3 = scanner(2);

        CompositeBatchScanner composite =
                new CompositeBatchScanner(Arrays.asList(s1, s2, s3), null);

        List<InternalRow> collected = collectAll(composite);

        assertThat(collected).hasSize(3);
        assertThat(intValues(collected)).containsExactlyInAnyOrder(0, 1, 2);
        assertThat(s1.closed).isTrue();
        assertThat(s2.closed).isTrue();
        assertThat(s3.closed).isTrue();
    }

    @Test
    void testPollBatchSkipsExhaustedScanner() throws IOException {
        // s1 is already exhausted (returns null immediately), s2 has data.
        StubBatchScanner s1 = scanner(); // no rows → immediately returns null
        StubBatchScanner s2 = scanner(99);

        CompositeBatchScanner composite = new CompositeBatchScanner(Arrays.asList(s1, s2), null);

        List<InternalRow> collected = collectAll(composite);

        assertThat(intValues(collected)).containsExactly(99);
        assertThat(s1.closed).isTrue();
        assertThat(s2.closed).isTrue();
    }

    @Test
    void testPollBatchWithLimit() throws IOException {
        // Two scanners with 3 rows each (one row per batch), limit = 3.
        // collectLimitedRows collects until rows.size() >= limit.
        StubBatchScanner s1 = scanner(1, 2, 3);
        StubBatchScanner s2 = scanner(4, 5, 6);

        CompositeBatchScanner composite = new CompositeBatchScanner(Arrays.asList(s1, s2), 3);

        CloseableIterator<InternalRow> batch = composite.pollBatch(TIMEOUT);
        assertThat(batch).isNotNull();

        List<Integer> values = new ArrayList<>();
        while (batch.hasNext()) {
            values.add(batch.next().getInt(0));
        }
        assertThat(values.size()).isGreaterThanOrEqualTo(3);
    }

    @Test
    void testCloseAllRemainingScannersInQueue() throws IOException {
        StubBatchScanner s1 = scanner(1, 2);
        StubBatchScanner s2 = scanner(3, 4);

        CompositeBatchScanner composite = new CompositeBatchScanner(Arrays.asList(s1, s2), null);

        // Poll one batch so s1 is re-enqueued and s2 stays in queue.
        composite.pollBatch(TIMEOUT);
        composite.close();

        assertThat(s1.closed).isTrue();
        assertThat(s2.closed).isTrue();
    }

    @Test
    void testCloseOnEmptyQueueDoesNotThrow() throws IOException {
        CompositeBatchScanner composite = new CompositeBatchScanner(Collections.emptyList(), null);
        composite.close(); // should not throw
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    /** Creates a {@link StubBatchScanner} that returns one row per {@code pollBatch} call. */
    private static StubBatchScanner scanner(int... values) {
        return new StubBatchScanner(values);
    }

    /** Drains the composite scanner and collects all rows. */
    private static List<InternalRow> collectAll(CompositeBatchScanner composite)
            throws IOException {
        List<InternalRow> rows = new ArrayList<>();
        CloseableIterator<InternalRow> batch;
        while ((batch = composite.pollBatch(TIMEOUT)) != null) {
            while (batch.hasNext()) {
                rows.add(batch.next());
            }
            batch.close();
        }
        return rows;
    }

    private static List<Integer> intValues(List<InternalRow> rows) {
        List<Integer> values = new ArrayList<>();
        for (InternalRow row : rows) {
            values.add(row.getInt(0));
        }
        return values;
    }

    // -------------------------------------------------------------------------
    // Stub
    // -------------------------------------------------------------------------

    /**
     * A stub {@link BatchScanner} that returns one row per {@link #pollBatch} call from a
     * pre-defined value list, then returns {@code null} once exhausted. Tracks whether it was
     * closed.
     */
    private static class StubBatchScanner implements BatchScanner {

        private final Queue<Integer> values;
        boolean closed = false;

        StubBatchScanner(int[] values) {
            this.values = new LinkedList<>();
            for (int v : values) {
                this.values.add(v);
            }
        }

        @Nullable
        @Override
        public CloseableIterator<InternalRow> pollBatch(Duration timeout) {
            if (values.isEmpty()) {
                return null;
            }
            GenericRow row = GenericRow.of(values.poll());
            return CloseableIterator.wrap(Collections.<InternalRow>singletonList(row).iterator());
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
