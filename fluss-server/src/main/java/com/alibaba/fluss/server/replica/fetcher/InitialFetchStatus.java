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

package com.alibaba.fluss.server.replica.fetcher;

import com.alibaba.fluss.metadata.TablePath;

import java.util.Objects;

/** Initial fetch state for specify table. */
public class InitialFetchStatus {
    private final long tableId;
    private final TablePath tablePath;
    private final int leader;
    private final long initOffset;

    public InitialFetchStatus(long tableId, TablePath tablePath, int leader, long initOffset) {
        this.tableId = tableId;
        this.tablePath = tablePath;
        this.leader = leader;
        this.initOffset = initOffset;
    }

    public long tableId() {
        return tableId;
    }

    public int leader() {
        return leader;
    }

    public TablePath tablePath() {
        return tablePath;
    }

    public long initOffset() {
        return initOffset;
    }

    @Override
    public String toString() {
        return "InitialFetchState{"
                + "tableId="
                + tableId
                + ", tablePath="
                + tablePath
                + ", leader="
                + leader
                + ", initOffset="
                + initOffset
                + '}';
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof InitialFetchStatus)) {
            return false;
        }
        InitialFetchStatus that = (InitialFetchStatus) object;
        return tableId == that.tableId
                && tablePath.equals(that.tablePath)
                && leader == that.leader
                && initOffset == that.initOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, tablePath, leader, initOffset);
    }
}
