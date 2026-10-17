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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ArrowScanRecords}. */
public class ArrowScanRecordsTest {
    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, 0);

    @Test
    void testFinishedOnlyBucketHasProgress() {
        try (ArrowScanRecords records =
                new ArrowScanRecords(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.singleton(TABLE_BUCKET))) {

            assertThat(records.count()).isZero();
            assertThat(records.hasProgress()).isTrue();
            assertThat(records.buckets()).containsExactly(TABLE_BUCKET);
            assertThat(records.records(TABLE_BUCKET)).isEmpty();
            assertThat(records.consumedUpToOffset(TABLE_BUCKET)).isNull();
            assertThat(records.finishedBuckets()).containsExactly(TABLE_BUCKET);
        }
    }
}
