/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.entity;

import com.alibaba.fluss.metadata.TablePath;

import java.util.Objects;

/** The info for the table assigned from Coordinator to lake tiering service to do tiering. */
public class LakeTieringTableInfo {

    private final long tableId;
    private final TablePath tablePath;
    private final long tieringEpoch;

    public LakeTieringTableInfo(long tableId, TablePath tablePath, long tieringEpoch) {
        this.tableId = tableId;
        this.tablePath = tablePath;
        this.tieringEpoch = tieringEpoch;
    }

    public long tableId() {
        return tableId;
    }

    public TablePath tablePath() {
        return tablePath;
    }

    public long tieringEpoch() {
        return tieringEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LakeTieringTableInfo that = (LakeTieringTableInfo) o;
        return tableId == that.tableId
                && tieringEpoch == that.tieringEpoch
                && Objects.equals(tablePath, that.tablePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, tablePath, tieringEpoch);
    }

    @Override
    public String toString() {
        return "LakeTieringTableInfo{"
                + "tableId="
                + tableId
                + ", tablePath="
                + tablePath
                + ", tieringEpoch="
                + tieringEpoch
                + '}';
    }
}
