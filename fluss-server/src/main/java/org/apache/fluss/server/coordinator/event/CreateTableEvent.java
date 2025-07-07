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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.server.zk.data.TableAssignment;

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
