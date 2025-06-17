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

package com.alibaba.fluss.server.entity;

import com.alibaba.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

/** The structure of fetch data. */
@Internal
// TODO rename to FetchReqInfo?
public final class FetchData {
    private final long tableId;
    private final long fetchOffset;
    @Nullable private final int[] projectFields;

    private int maxBytes;

    public FetchData(long tableId, long fetchOffset, int maxBytes) {
        this(tableId, fetchOffset, maxBytes, null);
    }

    public FetchData(long tableId, long fetchOffset, int maxBytes, @Nullable int[] projectFields) {
        this.tableId = tableId;
        this.fetchOffset = fetchOffset;
        this.maxBytes = maxBytes;
        this.projectFields = projectFields;
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
        FetchData fetchData = (FetchData) o;
        if (tableId != fetchData.tableId) {
            return false;
        }

        if (!Arrays.equals(projectFields, fetchData.projectFields)) {
            return false;
        }

        return fetchOffset == fetchData.fetchOffset && maxBytes == fetchData.maxBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, fetchOffset, maxBytes, Arrays.hashCode(projectFields));
    }
}
