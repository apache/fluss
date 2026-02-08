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

import org.apache.fluss.client.admin.KvSnapshotLease;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.time.Duration;

/**
 * Procedure to drop all kv snapshots leased of specified leaseId. See {@link
 * KvSnapshotLease#dropLease()} for more details.
 *
 * <p>Usage examples:
 *
 * <pre>
 * -- Drop kv snapshots leased of specified leaseId
 * CALL sys.drop_kv_snapshot_lease('test-lease-id');
 * </pre>
 */
public class DropKvSnapshotLeaseProcedure extends ProcedureBase {

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "leaseId", type = @DataTypeHint("STRING")),
            })
    public String[] call(ProcedureContext context, String leaseId) throws Exception {
        admin.createKvSnapshotLease(leaseId, Duration.ofDays(1).toMillis()).dropLease().get();
        return new String[] {"success"};
    }
}
