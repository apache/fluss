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

package org.apache.fluss.flink.tiering.committer;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Counts the consecutive failed commit rounds of each table.
 *
 * <p>A table leaves the counter when it commits a round, but a table that is dropped or no longer
 * tiered by this job after failing never does. The counter therefore keeps at most {@code
 * maxTables} tables and evicts the one that failed least recently, which restarts its count. A
 * table that keeps failing fails every round, so it is only evicted when more than {@code
 * maxTables} tables fail at the same time.
 */
@NotThreadSafe
final class ConsecutiveCommitFailureCounter {

    private final int maxTables;
    // Table ID -> consecutive failed rounds, ordered from the least to the most recent failure.
    private final LinkedHashMap<Long, Integer> failuresByTable = new LinkedHashMap<>();

    ConsecutiveCommitFailureCounter(int maxTables) {
        checkArgument(maxTables > 0, "maxTables must be positive, but is %s.", maxTables);
        this.maxTables = maxTables;
    }

    /** Records a failed round of the table and returns its consecutive failed rounds. */
    int recordFailure(long tableId) {
        // Re-insert to move the table to the most recent end.
        Integer previous = failuresByTable.remove(tableId);
        int failures = previous == null ? 1 : previous + 1;
        failuresByTable.put(tableId, failures);
        if (failuresByTable.size() > maxTables) {
            Iterator<Map.Entry<Long, Integer>> leastRecent = failuresByTable.entrySet().iterator();
            leastRecent.next();
            leastRecent.remove();
        }
        return failures;
    }

    /** Records a committed round of the table, which resets its count. */
    void recordSuccess(long tableId) {
        failuresByTable.remove(tableId);
    }

    /** Returns the consecutive failed rounds of the table, 0 if it is not failing. */
    int getFailures(long tableId) {
        return failuresByTable.getOrDefault(tableId, 0);
    }

    /** Returns the number of tables currently counted. */
    int size() {
        return failuresByTable.size();
    }
}
