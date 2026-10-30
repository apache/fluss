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

package org.apache.fluss.server.index;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

class IndexSendBufferTest {

    @Test
    void sourceCleanupReschedulesRemainingBatchAndFencesLateCompletion() throws Exception {
        IndexSendBuffer buffer = new IndexSendBuffer();
        Queue<TableBucket> ready = new ArrayDeque<>();
        buffer.setAppendListener(ready::add);
        TableBucket target = new TableBucket(2L, 0);
        try (IndexReplicator first = owner(buffer, 0);
                IndexReplicator second = owner(buffer, 1)) {
            IndexBatch firstBatch = batch(first, target);
            IndexBatch secondBatch = batch(second, target);
            assertThat(buffer.tryAppendWindow(Collections.singletonList(firstBatch))).isTrue();
            assertThat(buffer.claimFirstReady(ready.remove(), 0L)).isSameAs(firstBatch);
            assertThat(buffer.tryAppendWindow(Collections.singletonList(secondBatch))).isTrue();
            assertThat(buffer.hasClaim(ready.remove())).isTrue();
            assertThat(ready).isEmpty();

            first.close();

            assertThat(ready).containsExactly(target);
            assertThat(buffer.pendingBytes()).isEqualTo(1L);
            assertThat(buffer.claimFirstReady(ready.remove(), 0L)).isSameAs(secondBatch);
            assertThat(buffer.acknowledgeClaim(firstBatch)).isFalse();
            assertThat(buffer.retryClaim(firstBatch, 100L)).isFalse();
            assertThat(buffer.ownsClaim(secondBatch)).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(1L);
            assertThat(buffer.acknowledgeClaim(secondBatch)).isTrue();
            secondBatch.window().onBatchAcked();
            assertThat(second.getAllIndexPushedOffset()).isEqualTo(1L);
            assertThat(buffer.pendingBytes()).isZero();
            assertThat(buffer.buckets()).isEmpty();
        }
    }

    private static IndexBatch batch(IndexReplicator owner, TableBucket target) {
        return new IndexBatch(
                target,
                owner.sourceBucket(),
                1L,
                new byte[0],
                new MemorySegmentBytesView(MemorySegment.wrap(new byte[1]), 0, 1),
                1L,
                new IndexReplicationWindow("idx", 1L, 1, owner));
    }

    private static IndexReplicator owner(IndexSendBuffer buffer, int bucket) {
        IndexSourceReader.SourceLog log =
                new IndexSourceReader.SourceLog() {
                    @Override
                    public TableBucket tableBucket() {
                        return new TableBucket(1L, bucket);
                    }

                    @Override
                    public long highWatermark() {
                        return 1L;
                    }

                    @Override
                    public long logStartOffset() {
                        return 0L;
                    }

                    @Override
                    public FetchDataInfo read(
                            long offset,
                            int maxBytes,
                            FetchIsolation isolation,
                            boolean minOneMessage) {
                        throw new UnsupportedOperationException();
                    }
                };
        LogRecordReadContext context =
                LogRecordReadContext.createCompactedRowReadContext(
                        RowType.of(DataTypes.INT()), 1, null);
        return new IndexReplicator(
                new IndexSourceReader(log, null, Runnable::run, context),
                Collections.emptyList(),
                buffer,
                context,
                0L,
                1024,
                1024,
                (syncOffset, allOffset) -> {});
    }
}
