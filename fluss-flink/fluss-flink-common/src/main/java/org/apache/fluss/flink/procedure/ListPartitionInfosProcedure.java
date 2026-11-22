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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Procedure to list the partition id, partition name and bucket count of each partition of a
 * partitioned table.
 *
 * <p>After an {@code ALTER TABLE ... SET ('bucket.num' = N)}, existing partitions keep their
 * original bucket count while only newly created partitions use the new one. This procedure makes
 * the per-partition bucket counts visible from Flink SQL; the table-level {@code bucket.num} shown
 * by {@code SHOW CREATE TABLE} is only the default for newly created partitions.
 *
 * <p>Usage example:
 *
 * <pre>
 * -- List the partition id, partition name and bucket count of each partition
 * CALL sys.list_partition_infos('my_db', 'my_table');
 * </pre>
 */
public class ListPartitionInfosProcedure extends ProcedureBase {

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "db", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "table_name", type = @DataTypeHint("STRING"))
            },
            output =
                    @DataTypeHint(
                            "ROW<partition_id BIGINT, partition_name STRING, bucket_count INT>"))
    public Row[] call(ProcedureContext context, String db, String tableName) throws Exception {
        List<PartitionInfo> partitionInfos =
                admin.listPartitionInfos(TablePath.of(db, tableName)).get();
        return partitionInfos.stream()
                .map(p -> Row.of(p.getPartitionId(), p.getPartitionName(), p.getBucketCount()))
                .toArray(Row[]::new);
    }
}
