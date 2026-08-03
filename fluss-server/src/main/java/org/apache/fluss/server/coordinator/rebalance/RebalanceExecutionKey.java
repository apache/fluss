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

package org.apache.fluss.server.coordinator.rebalance;

import org.apache.fluss.metadata.TableBucket;

import java.util.Objects;

/** Identifies one execution attempt of a bucket-level rebalance task. */
public final class RebalanceExecutionKey {
    private final String rebalanceId;
    private final TableBucket tableBucket;
    private final long attemptId;

    public RebalanceExecutionKey(String rebalanceId, TableBucket tableBucket, long attemptId) {
        this.rebalanceId = rebalanceId;
        this.tableBucket = tableBucket;
        this.attemptId = attemptId;
    }

    public String getRebalanceId() {
        return rebalanceId;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public long getAttemptId() {
        return attemptId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RebalanceExecutionKey that = (RebalanceExecutionKey) o;
        return attemptId == that.attemptId
                && Objects.equals(rebalanceId, that.rebalanceId)
                && Objects.equals(tableBucket, that.tableBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rebalanceId, tableBucket, attemptId);
    }

    @Override
    public String toString() {
        return "RebalanceExecutionKey{"
                + "rebalanceId='"
                + rebalanceId
                + '\''
                + ", tableBucket="
                + tableBucket
                + ", attemptId="
                + attemptId
                + '}';
    }
}
