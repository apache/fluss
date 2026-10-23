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

import org.apache.fluss.cluster.compaction.CompactionInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import java.util.List;

/** Procedure to list retained manual compaction summaries associated with a scope. */
public class ListCompactionsProcedure extends ProcedureBase {

    private static final String OUTPUT_TYPE =
            "ROW<operation_id STRING, compaction_type STRING, compaction_status STRING, database_name STRING, table_name STRING, partition_name STRING, create_time BIGINT>";

    /** Lists compactions associated with the cluster scope. */
    @ProcedureHint(output = @DataTypeHint(OUTPUT_TYPE))
    public Row[] call(ProcedureContext context) throws Exception {
        return toRows(admin.listCompactions().get());
    }

    /** Lists compactions associated with the database scope. */
    @ProcedureHint(
            argument = {@ArgumentHint(name = "database", type = @DataTypeHint("STRING"))},
            output = @DataTypeHint(OUTPUT_TYPE))
    public Row[] call(ProcedureContext context, String database) throws Exception {
        return toRows(admin.listCompactions(database).get());
    }

    /** Lists compactions associated with the table scope. */
    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "database", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING"))
            },
            output = @DataTypeHint(OUTPUT_TYPE))
    public Row[] call(ProcedureContext context, String database, String table) throws Exception {
        return toRows(admin.listCompactions(TablePath.of(database, table)).get());
    }

    /** Lists compactions associated with the partition scope. */
    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "database", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "partition", type = @DataTypeHint("STRING"))
            },
            output = @DataTypeHint(OUTPUT_TYPE))
    public Row[] call(ProcedureContext context, String database, String table, String partition)
            throws Exception {
        return toRows(admin.listCompactions(TablePath.of(database, table), partition).get());
    }

    private static Row[] toRows(List<CompactionInfo> infos) {
        return infos.stream()
                .map(
                        info ->
                                Row.of(
                                        info.operationId(),
                                        info.type().name(),
                                        info.status().name(),
                                        info.databaseName().orElse(null),
                                        info.tablePath().map(TablePath::getTableName).orElse(null),
                                        info.partitionName().orElse(null),
                                        info.createTime()))
                .toArray(Row[]::new);
    }
}
