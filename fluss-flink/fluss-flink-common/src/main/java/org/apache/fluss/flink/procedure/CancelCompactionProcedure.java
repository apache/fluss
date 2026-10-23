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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

/** Procedure to cancel a manual compaction operation by ID. */
public class CancelCompactionProcedure extends ProcedureBase {

    /** Cancels the operation and returns after the cancellation response. */
    @ProcedureHint(
            argument = {@ArgumentHint(name = "operation_id", type = @DataTypeHint("STRING"))},
            output = @DataTypeHint("ROW<result STRING>"))
    public Row[] call(ProcedureContext context, String operationId) throws Exception {
        admin.cancelCompaction(operationId).get();
        return new Row[] {Row.of("success")};
    }
}
