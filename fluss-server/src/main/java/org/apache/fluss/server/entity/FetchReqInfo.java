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

package org.apache.fluss.server.entity;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

/** The structure of fetch data. */
@Internal
public final class FetchReqInfo {
    private final long tableId;
    private final long fetchOffset;
    @Nullable private final int[] projectFields;

    private int maxBytes;
    private final int currentLeaderEpoch;
    private final int lastFetchedEpoch;

    public FetchReqInfo(long tableId, long fetchOffset, int maxBytes) {
        this(tableId, fetchOffset, maxBytes, null);
    }

    public FetchReqInfo(
            long tableId, long fetchOffset, int maxBytes, @Nullable int[] projectFields) {
        this(tableId, fetchOffset, maxBytes, projectFields, -1, -1);
    }

    public FetchReqInfo(
            long tableId,
            long fetchOffset,
            int maxBytes,
            @Nullable int[] projectFields,
            int currentLeaderEpoch,
            int lastFetchedEpoch) {
        this.currentLeaderEpoch = currentLeaderEpoch;
        this.lastFetchedEpoch = lastFetchedEpoch;
        this.tableId = tableId;
        this.fetchOffset = fetchOffset;
        this.maxBytes = maxBytes;
        this.projectFields = projectFields;
    }

    public int currentLeaderEpoch() {
        return currentLeaderEpoch;
    }

    public int lastFetchedEpoch() {
        return lastFetchedEpoch;
    }

    public long getTableId() {
        return tableId;
    }

    public long getFetchOffset() {
        return fetchOffset;
    }

    public void setFetchMaxBytes(int maxBytes) {
        this.maxBytes = maxBytes;
    }

    public int getMaxBytes() {
        return maxBytes;
    }

    @Nullable
    public int[] getProjectFields() {
        return projectFields;
    }

    @Override
    public String toString() {
        return "FetchData{"
                + "tableId="
                + tableId
                + ", fetchOffset="
                + fetchOffset
                + ", maxBytes ="
                + maxBytes
                + ", projectionFields="
                + Arrays.toString(projectFields)
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FetchReqInfo fetchReqInfo = (FetchReqInfo) o;
        if (tableId != fetchReqInfo.tableId) {
            return false;
        }

        if (!Arrays.equals(projectFields, fetchReqInfo.projectFields)) {
            return false;
        }

        return fetchOffset == fetchReqInfo.fetchOffset
                && maxBytes == fetchReqInfo.maxBytes
                && currentLeaderEpoch == fetchReqInfo.currentLeaderEpoch
                && lastFetchedEpoch == fetchReqInfo.lastFetchedEpoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableId,
                fetchOffset,
                maxBytes,
                Arrays.hashCode(projectFields),
                currentLeaderEpoch,
                lastFetchedEpoch);
    }
}
