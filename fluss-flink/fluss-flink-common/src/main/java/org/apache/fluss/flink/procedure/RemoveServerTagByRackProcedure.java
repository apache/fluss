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

import org.apache.fluss.cluster.rebalance.ServerTag;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.List;

import static org.apache.fluss.flink.procedure.AddServerTagByRackProcedure.validateAndGetRacks;
import static org.apache.fluss.flink.procedure.RemoveServerTagProcedure.validateAndGetServerTag;

/**
 * Procedure to remove server tag by rack.
 *
 * <p>Usage:
 *
 * <pre><code>
 *  CALL sys.remove_server_tag_by_rack('rack-0', 'PERMANENT_OFFLINE')
 *  CALL sys.remove_server_tag_by_rack('rack-0,rack-1', 'TEMPORARY_OFFLINE')
 * </code></pre>
 */
public class RemoveServerTagByRackProcedure extends ProcedureBase {

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "racks", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "serverTag", type = @DataTypeHint("STRING"))
            })
    public String[] call(ProcedureContext context, String racks, String serverTag)
            throws Exception {
        List<String> rackList = validateAndGetRacks(racks);
        ServerTag tag = validateAndGetServerTag(serverTag);
        admin.removeServerTagByRack(rackList, tag).get();
        return new String[] {"success"};
    }
}
