/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.rpc.entity;

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.record.MemoryLogRecords.EMPTY;
import static org.apache.fluss.rpc.entity.FetchLogResultForBucket.NO_RESOLVED_EARLIEST_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;

/** Test FetchLogResultForBucket. */
public class FetchLogResultForBucketTest {
    @Test
    void testResolvedEarliestOffsetPresence() {
        TableBucket bucket = new TableBucket(1L, 0);

        FetchLogResultForBucket absent =
                FetchLogResultForBucket.records(bucket, EMPTY, 10L, -1L, -1L);

        assertThat(absent.hasResolvedEarliestOffset()).isFalse();
        assertThat(absent.getResolvedEarliestOffset()).isEqualTo(NO_RESOLVED_EARLIEST_OFFSET);

        FetchLogResultForBucket zero =
                FetchLogResultForBucket.records(bucket, EMPTY, 10L, -1L, -1L, 0L);

        assertThat(zero.hasResolvedEarliestOffset()).isTrue();
        assertThat(zero.getResolvedEarliestOffset()).isZero();

        FetchLogResultForBucket retained =
                FetchLogResultForBucket.records(bucket, EMPTY, 100L, -1L, -1L, 100L);

        assertThat(retained.getResolvedEarliestOffset()).isEqualTo(100L);
    }
}
