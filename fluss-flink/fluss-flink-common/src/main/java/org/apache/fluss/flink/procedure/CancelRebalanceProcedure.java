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

import org.apache.fluss.client.admin.Admin;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

/**
 * Procedure to cancel rebalance.
 *
 * <p>This procedure allows canceling rebalance. See {@link Admin#cancelRebalance(String)} for more
 * details.
 *
 * <pre>
 * -- Cancel the rebalance without rebalance id
 * CALL sys.cancel_rebalance();
 *
 * -- Cancel the rebalance with rebalance id
 * CALL sys.cancel_rebalance('xxx_xxx_xxx');
 * </pre>
 */
public class CancelRebalanceProcedure extends ProcedureBase {

    @ProcedureHint(
            argument = {
                @ArgumentHint(
                        name = "rebalanceId",
                        type = @DataTypeHint("STRING"),
                        isOptional = true)
            })
    public String[] call(ProcedureContext context, @Nullable String rebalanceId) throws Exception {
        admin.cancelRebalance(rebalanceId).get();
        return new String[] {"success"};
    }
}
