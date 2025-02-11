/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator.event;

import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.server.zk.data.TableAssignment;

import java.util.Objects;

/** An event for create table. */
public class CreateTableEvent implements CoordinatorEvent {

    private final TableInfo tableInfo;
    private final TableAssignment tableAssignment;

    public CreateTableEvent(TableInfo tableInfo, TableAssignment tableAssignment) {
        this.tableInfo = tableInfo;
        this.tableAssignment = tableAssignment;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public TableAssignment getTableAssignment() {
        return tableAssignment;
    }

    public boolean isAutoPartitionTable() {
        return tableInfo.isAutoPartitioned();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CreateTableEvent that = (CreateTableEvent) o;
        return Objects.equals(tableInfo, that.tableInfo)
                && Objects.equals(tableAssignment, that.tableAssignment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableInfo, tableAssignment);
    }

    @Override
    public String toString() {
        return "CreateTableEvent{"
                + "tableInfo="
                + tableInfo
                + ", tableAssignment="
                + tableAssignment
                + '}';
    }
}
