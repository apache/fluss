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

package org.apache.fluss.lake.source;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.types.RowType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Optional lake capability used to select a complete base for Spark historical reads. */
@Internal
public interface SupportsHistoricalSnapshot {

    /** Pins the expected logical schema for historical readers; mismatches must fail. */
    void withExpectedSchema(RowType expectedSchema);

    /** Finds the newest retained base whose replay interval is complete for every bucket. */
    Optional<HistoricalSnapshot> findHistoricalSnapshot(
            TableInfo tableInfo,
            Map<TableBucket, Long> earliestOffsets,
            Map<TableBucket, Long> targetOffsets)
            throws IOException;

    /** A fixed snapshot and the exclusive Fluss offsets of its complete readable state. */
    final class HistoricalSnapshot {
        private final long snapshotId;
        private final Map<TableBucket, Long> readableOffsets;

        public HistoricalSnapshot(long snapshotId, Map<TableBucket, Long> readableOffsets) {
            this.snapshotId = snapshotId;
            this.readableOffsets = Collections.unmodifiableMap(new HashMap<>(readableOffsets));
        }

        public long getSnapshotId() {
            return snapshotId;
        }

        public Map<TableBucket, Long> getReadableOffsets() {
            return readableOffsets;
        }
    }
}
