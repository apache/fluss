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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link LogScannerStatus}. */
public class LogScannerStatusTest {

    @Test
    void testRecordsLagAggregationAndUnassignment() {
        LogScannerStatus status = new LogScannerStatus();
        TableBucket firstBucket = new TableBucket(1L, 0);
        TableBucket secondBucket = new TableBucket(1L, 1);
        TableBucket boundedBucket = new TableBucket(1L, 2);

        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(firstBucket, 0L);
        offsets.put(secondBucket, 0L);
        offsets.put(boundedBucket, 0L);
        status.assignScanBuckets(offsets);

        // Lag is unknown before high watermarks are available.
        assertThat(status.recordsLag()).isZero();

        status.updateHighWatermark(firstBucket, 5L);
        status.updateHighWatermark(secondBucket, 5L);
        status.updateHighWatermark(boundedBucket, 5L);

        status.updateOffset(firstBucket, 1L);
        status.updateOffset(secondBucket, 1L);
        status.updateOffset(boundedBucket, 1L);
        assertThat(status.recordsLag()).isEqualTo(12L);

        status.unassignScanBuckets(Arrays.asList(boundedBucket));
        assertThat(status.recordsLag()).isEqualTo(8L);

        status.updateOffset(firstBucket, 5L);
        assertThat(status.recordsLag()).isEqualTo(4L);

        status.updateOffset(secondBucket, 5L);
        assertThat(status.recordsLag()).isZero();
    }
}
