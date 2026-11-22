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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.apache.fluss.trino.FlussSplitReader.PollResult.AVAILABLE;
import static org.apache.fluss.trino.FlussSplitReader.PollResult.FINISHED;
import static org.apache.fluss.trino.FlussSplitReader.PollResult.YIELD;
import static org.apache.fluss.trino.TestingRowIterator.rows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies the KV batch iterator protocol independently of Trino page assembly. */
final class FlussKvSplitReaderTest extends FlussTrinoTestBase {
    @Test
    void testEmptyBatchYieldsAndOnlyNullFinishes() throws Exception {
        TablePath path = TablePath.of("fluss", "kv_reader");
        TableInfo info =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.BIGINT())
                                                .primaryKey("id")
                                                .build())
                                .distributedBy(1)
                                .build());
        FlussBucketHandle bucket = new FlussBucketHandle(info.getTableId(), Optional.empty(), 0);
        TestingRowIterator empty = rows(0);
        TestingRowIterator rows = rows(1);
        TestingBatchScanner scanner = new TestingBatchScanner(empty, rows);
        Duration timeout = Duration.ofMillis(100);
        try (Table table =
                        new TestingScanTable(catalogTable(path)) {
                            @Override
                            BatchScanner createBatchScanner(Scan scan, TableBucket requested) {
                                assertThat(requested).isEqualTo(bucket.toTableBucket());
                                return scanner;
                            }
                        };
                FlussKvSplitReader reader = new FlussKvSplitReader(table, bucket)) {
            assertThat(reader.poll(timeout)).isEqualTo(YIELD);
            assertThat(empty.closes).isEqualTo(1);
            assertThat(reader.isFinished()).isFalse();
            assertThat(reader.poll(timeout)).isEqualTo(AVAILABLE);
            assertThatThrownBy(() -> reader.poll(timeout))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(reader.next().getLong(0)).isZero();
            assertThat(rows.closes).isEqualTo(1);
            assertThat(reader.hasNext()).isFalse();
            assertThat(reader.isFinished()).isFalse();
            assertThat(reader.getCompletedBytes()).isZero();
            assertThat(reader.getRetainedSizeInBytes()).isZero();
            assertThat(reader.poll(timeout)).isEqualTo(FINISHED);
        }
        assertThat(scanner.closes).isEqualTo(1);
        assertThat(scanner.polls).isEqualTo(3);
    }
}
