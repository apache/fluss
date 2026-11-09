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

package org.apache.fluss.trino;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import static org.apache.fluss.trino.FlussSplitReader.PollResult.AVAILABLE;
import static org.apache.fluss.trino.FlussSplitReader.PollResult.FINISHED;
import static org.apache.fluss.trino.FlussSplitReader.PollResult.YIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** Verifies the KV batch iterator protocol independently of Trino page assembly. */
final class FlussKvSplitReaderTest {
    @Test
    void testEmptyBatchYieldsAndOnlyNullFinishes() throws Exception {
        FlussBucketHandle bucket = new FlussBucketHandle(42, Optional.of(7L), 2);
        Table table = mock(Table.class);
        Scan scan = mock(Scan.class);
        BatchScanner scanner = mock(BatchScanner.class);
        when(table.newScan()).thenReturn(scan);
        when(scan.createBatchScanner(bucket.toTableBucket())).thenReturn(scanner);
        CloseableIterator<InternalRow> empty = spy(CloseableIterator.emptyIterator());
        CloseableIterator<InternalRow> rows =
                spy(
                        CloseableIterator.wrap(
                                Collections.<InternalRow>singletonList(GenericRow.of(1L))
                                        .iterator()));
        Duration timeout = Duration.ofMillis(100);
        when(scanner.pollBatch(timeout)).thenReturn(empty, rows, null);
        try (FlussKvSplitReader reader = new FlussKvSplitReader(table, bucket)) {
            assertThat(reader.poll(timeout)).isEqualTo(YIELD);
            verify(empty).close();
            assertThat(reader.isFinished()).isFalse();
            assertThat(reader.poll(timeout)).isEqualTo(AVAILABLE);
            assertThatThrownBy(() -> reader.poll(timeout))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(reader.next().getLong(0)).isEqualTo(1);
            verify(rows).close();
            assertThat(reader.hasNext()).isFalse();
            assertThat(reader.isFinished()).isFalse();
            assertThat(reader.getCompletedBytes()).isZero();
            assertThat(reader.getRetainedSizeInBytes()).isZero();
            assertThat(reader.poll(timeout)).isEqualTo(FINISHED);
        }
        verify(scan).createBatchScanner(bucket.toTableBucket());
        verifyNoMoreInteractions(scan);
        verify(scanner).close();
    }
}
