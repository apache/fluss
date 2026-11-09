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

import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KvSnapshotAndLogBatchScanner}. */
class KvSnapshotAndLogBatchScannerTest {

    private static final Duration TIMEOUT = Duration.ofMillis(10);

    @Test
    void testSnapshotRecordIteratorPollsAfterEmptyBatch() throws Exception {
        StubBatchScanner scanner =
                new StubBatchScanner(
                        Arrays.asList(
                                Collections.<InternalRow>emptyList(),
                                Collections.<InternalRow>singletonList(GenericRow.of(1)),
                                Collections.<InternalRow>emptyList(),
                                Collections.<InternalRow>singletonList(GenericRow.of(2))));

        KvSnapshotAndLogBatchScanner.SnapshotRecordIterator iterator =
                new KvSnapshotAndLogBatchScanner.SnapshotRecordIterator(scanner, TIMEOUT);

        List<Integer> values = new ArrayList<>();
        while (iterator.hasNext()) {
            LogRecord record = iterator.next();
            values.add(record.getRow().getInt(0));
        }

        assertThat(values).containsExactly(1, 2);
        assertThat(scanner.pollCount).isEqualTo(5);
    }

    private static class StubBatchScanner implements BatchScanner {

        private final Queue<List<InternalRow>> batches;
        private int pollCount;

        StubBatchScanner(List<List<InternalRow>> batches) {
            this.batches = new LinkedBlockingQueue<>(batches);
        }

        @Nullable
        @Override
        public CloseableIterator<InternalRow> pollBatch(Duration timeout) {
            pollCount++;
            if (batches.isEmpty()) {
                return null;
            }
            return CloseableIterator.wrap(batches.poll().iterator());
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
