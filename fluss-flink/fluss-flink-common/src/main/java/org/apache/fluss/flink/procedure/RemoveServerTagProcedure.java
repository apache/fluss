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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Procedure to remove server tags. */
public class RemoveServerTagProcedure extends ProcedureBase {

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "tabletServers", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "serverTag", type = @DataTypeHint("STRING"))
            })
    public String[] call(ProcedureContext context, String tabletServers, String serverTag)
            throws Exception {
        List<Integer> servers = validateAndGetTabletServers(tabletServers);
        ServerTag tag = validateAndGetServerTag(serverTag);
        admin.removeServerTag(servers, tag).get();
        return new String[] {"success"};
    }

    public static List<Integer> validateAndGetTabletServers(String tabletServers) {
        if (tabletServers == null || tabletServers.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "tabletServers cannot be null or empty. You can specify one tabletServer as 1 or "
                            + "specify multi tabletServers as 1;2 (split by ';')");
        }

        tabletServers = tabletServers.trim();
        String[] splitServers = tabletServers.split(",");
        if (splitServers.length == 0) {
            throw new IllegalArgumentException(
                    "tabletServers cannot be empty. You can specify one tabletServer as 1 or "
                            + "specify multi tabletServers as 1,2 (split by ',')");
        }
        List<Integer> servers = new ArrayList<>();
        for (String server : splitServers) {
            servers.add(Integer.parseInt(server));
        }
        return servers;
    }

    public static ServerTag validateAndGetServerTag(String serverTag) {
        if (serverTag == null || serverTag.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "serverTag cannot be null or empty. Please specify a valid serverTag. serverTag is one of "
                            + Arrays.asList(ServerTag.values()));
        }
        serverTag = serverTag.trim().toUpperCase();
        return ServerTag.valueOf(serverTag);
    }
}
