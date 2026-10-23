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

import org.apache.fluss.cluster.compaction.CompactionType;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

/** Procedure to submit manual compaction and return its operation ID. */
public class CompactProcedure extends ProcedureBase {

    /** Submits cluster compaction without waiting for operation completion. */
    @ProcedureHint(
            argument = {@ArgumentHint(name = "type", type = @DataTypeHint("STRING"))},
            output = @DataTypeHint("ROW<operation_id STRING>"))
    public Row[] call(ProcedureContext context, String type) throws Exception {
        return new Row[] {Row.of(admin.compact(parseType(type)).get())};
    }

    /** Submits database compaction without waiting for operation completion. */
    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "database", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "type", type = @DataTypeHint("STRING"))
            },
            output = @DataTypeHint("ROW<operation_id STRING>"))
    public Row[] call(ProcedureContext context, String database, String type) throws Exception {
        return new Row[] {Row.of(admin.compact(database, parseType(type)).get())};
    }

    /** Submits table compaction without waiting for operation completion. */
    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "database", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "type", type = @DataTypeHint("STRING"))
            },
            output = @DataTypeHint("ROW<operation_id STRING>"))
    public Row[] call(ProcedureContext context, String database, String table, String type)
            throws Exception {
        return new Row[] {
            Row.of(admin.compact(TablePath.of(database, table), parseType(type)).get())
        };
    }

    /** Submits partition compaction without waiting for operation completion. */
    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "database", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "partition", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "type", type = @DataTypeHint("STRING"))
            },
            output = @DataTypeHint("ROW<operation_id STRING>"))
    public Row[] call(
            ProcedureContext context, String database, String table, String partition, String type)
            throws Exception {
        return new Row[] {
            Row.of(admin.compact(TablePath.of(database, table), partition, parseType(type)).get())
        };
    }

    private static CompactionType parseType(String type) {
        try {
            return CompactionType.valueOf(type);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IllegalArgumentException(
                    "Unknown compaction type: "
                            + type
                            + ". Expected OPTIMIZE, RECLAIM, or MIGRATE.");
        }
    }
}
