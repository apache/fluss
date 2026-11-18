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

import org.apache.fluss.client.table.ReadDirection;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.OrderedKeyEncoder;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests ordering and cleanup when a merge input cannot immediately produce its next key. */
class OrderedBatchScannerTest {
    private final OrderedKeyEncoder encoder =
            OrderedKeyEncoder.createKeyEncoder(
                    new RowType(Collections.singletonList(new DataField("k", DataTypes.INT()))),
                    Collections.singletonList("k"));

    @Test
    void waitsForEachHeadAndAppliesGlobalLimit() throws IOException {
        TestingInput left = new TestingInput();
        left.add(2, 4);
        TestingInput right = new TestingInput();
        right.add();
        right.add(1);
        right.add();
        right.add(3, 5);

        try (BatchScanner scanner =
                new OrderedBatchScanner(
                        Arrays.asList(left, right), encoder, ReadDirection.FORWARD, null, 3)) {
            assertThat(BatchScanUtils.toList(scanner.pollBatch(Duration.ZERO))).isEmpty();
            assertThat(keys(BatchScanUtils.toList(scanner.pollBatch(Duration.ZERO))))
                    .containsExactly(1);
            assertThat(keys(BatchScanUtils.toList(scanner.pollBatch(Duration.ZERO))))
                    .containsExactly(2, 3);
            assertThat(scanner.pollBatch(Duration.ZERO)).isNull();
            assertThat(left.closed).isTrue();
            assertThat(right.closed).isTrue();
        }
    }

    @Test
    void closesEveryInputOnPollFailure() throws IOException {
        TestingInput left = new TestingInput();
        left.add(1);
        TestingInput failed = new TestingInput();
        failed.fail = true;
        TestingInput unopened = new TestingInput();

        try (BatchScanner scanner =
                new OrderedBatchScanner(
                        Arrays.asList(left, failed, unopened),
                        encoder,
                        ReadDirection.FORWARD,
                        null,
                        null)) {
            assertThatThrownBy(() -> scanner.pollBatch(Duration.ZERO))
                    .isInstanceOf(IOException.class)
                    .hasMessage("lost bucket session");
            assertThat(left.closed).isTrue();
            assertThat(failed.closed).isTrue();
            assertThat(unopened.closed).isTrue();
            assertThat(scanner.pollBatch(Duration.ZERO)).isNull();
        }
    }

    private static List<Integer> keys(List<InternalRow> rows) {
        return rows.stream().map(row -> row.getInt(0)).collect(Collectors.toList());
    }

    private static final class TestingInput implements BatchScanner {
        private final Queue<List<InternalRow>> batches = new ArrayDeque<>();
        private boolean closed;
        private boolean fail;

        private void add(Integer... keys) {
            batches.add(
                    Arrays.stream(keys)
                            .map(key -> GenericRow.of(key))
                            .collect(Collectors.toList()));
        }

        @Override
        public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
            if (fail) {
                throw new IOException("lost bucket session");
            }
            List<InternalRow> rows = batches.poll();
            return rows == null ? null : CloseableIterator.wrap(rows.iterator());
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
