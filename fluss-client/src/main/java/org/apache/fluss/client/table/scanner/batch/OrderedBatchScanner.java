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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.table.ReadDirection;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.encode.OrderedKeyEncoder;
import org.apache.fluss.shaded.guava32.com.google.common.primitives.UnsignedBytes;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** Merges fixed-view bucket scans in physical primary key order. */
@Internal
public final class OrderedBatchScanner implements BatchScanner {
    private static final int MAX_BATCH_ROWS = 1024;

    private final List<Input> inputs = new ArrayList<>();
    private final ArrayDeque<Input> pending = new ArrayDeque<>();
    private final PriorityQueue<Input> heads;
    private final OrderedKeyEncoder encoder;
    @Nullable private final int[] projection;
    private long remaining;
    private boolean closed;

    /** Creates a merge over ordered child scanners and owns their lifetime. */
    public OrderedBatchScanner(
            List<BatchScanner> scanners,
            OrderedKeyEncoder encoder,
            ReadDirection direction,
            @Nullable int[] projection,
            @Nullable Integer limit) {
        Comparator<byte[]> keyOrder = UnsignedBytes.lexicographicalComparator();
        Comparator<Input> order = (left, right) -> keyOrder.compare(left.key, right.key);
        heads = new PriorityQueue<>(direction == ReadDirection.REVERSE ? order.reversed() : order);
        this.encoder = encoder;
        this.projection = projection;
        remaining = limit == null ? Long.MAX_VALUE : limit;
        for (BatchScanner scanner : scanners) {
            Input input = new Input(scanner);
            inputs.add(input);
            pending.add(input);
        }
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (closed) {
            return null;
        }
        List<InternalRow> rows = new ArrayList<>();
        long start = System.nanoTime();
        try {
            while (rows.size() < MAX_BATCH_ROWS && remaining > 0) {
                while (!pending.isEmpty()) {
                    Input input = pending.peek();
                    Duration budget = timeout.minusNanos(System.nanoTime() - start);
                    if (!input.loadHead(
                            rows.isEmpty() && !budget.isNegative() ? budget : Duration.ZERO)) {
                        return CloseableIterator.wrap(rows.iterator());
                    }
                    pending.remove();
                    if (input.row != null) {
                        heads.add(input);
                    }
                }
                if (heads.isEmpty()) {
                    close();
                    break;
                }
                Input input = heads.remove();
                InternalRow row = input.row;
                rows.add(projection == null ? row : ProjectedRow.from(projection).replaceRow(row));
                remaining--;
                if (remaining > 0) {
                    pending.add(input);
                }
            }
            if (remaining == 0) {
                close();
            }
            return rows.isEmpty() && closed ? null : CloseableIterator.wrap(rows.iterator());
        } catch (IOException | RuntimeException e) {
            close();
            throw e;
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            for (Input input : inputs) {
                input.close();
            }
            heads.clear();
            pending.clear();
            inputs.clear();
        }
    }

    private final class Input {
        private final BatchScanner scanner;
        @Nullable private CloseableIterator<InternalRow> batch;
        @Nullable private InternalRow row;
        @Nullable private byte[] key;
        private boolean done;

        private Input(BatchScanner scanner) {
            this.scanner = scanner;
        }

        private boolean loadHead(Duration timeout) throws IOException {
            if (batch == null || !batch.hasNext()) {
                IOUtils.closeQuietly(batch);
                batch = scanner.pollBatch(timeout);
            }
            if (batch == null) {
                close();
                return true;
            }
            if (!batch.hasNext()) {
                return false;
            }
            row = batch.next();
            key = encoder.encodeKey(row);
            return true;
        }

        private void close() {
            if (!done) {
                done = true;
                IOUtils.closeQuietly(batch);
                IOUtils.closeQuietly(scanner);
                batch = null;
                row = null;
                key = null;
            }
        }
    }
}
