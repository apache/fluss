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

package com.alibaba.fluss.metadata;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.utils.json.JsonSerdeUtils;
import com.alibaba.fluss.utils.json.TablePartitionJsonSerde;

import java.util.Objects;

/**
 * A class to identify a table partition, containing the table id and the partition id.
 *
 * @since 0.2
 */
@PublicEvolving
public class TablePartition {

    private final long tableId;
    private final long partitionId;

    public TablePartition(long tableId, long partitionId) {
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public byte[] toJsonBytes() {
        return JsonSerdeUtils.writeValueAsBytes(this, TablePartitionJsonSerde.INSTANCE);
    }

    public static TablePartition fromJsonBytes(byte[] json) {
        return JsonSerdeUtils.readValue(json, TablePartitionJsonSerde.INSTANCE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TablePartition that = (TablePartition) o;
        return tableId == that.tableId && Objects.equals(partitionId, that.partitionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, partitionId);
    }

    @Override
    public String toString() {
        return "TablePartition{" + "tableId=" + tableId + ", partitionId=" + partitionId + '}';
    }
}
