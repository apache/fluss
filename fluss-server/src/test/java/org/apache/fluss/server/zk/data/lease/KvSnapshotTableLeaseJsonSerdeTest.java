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

package org.apache.fluss.server.zk.data.lease;

import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.HashMap;
import java.util.Map;

/** Test for {@link KvSnapshotTableLeaseJsonSerde}. */
public class KvSnapshotTableLeaseJsonSerdeTest extends JsonSerdeTestBase<KvSnapshotTableLease> {

    KvSnapshotTableLeaseJsonSerdeTest() {
        super(KvSnapshotTableLeaseJsonSerde.INSTANCE);
    }

    @Override
    protected KvSnapshotTableLease[] createObjects() {
        KvSnapshotTableLease[] kvSnapshotTableLeases = new KvSnapshotTableLease[2];
        kvSnapshotTableLeases[0] = new KvSnapshotTableLease(1L, new Long[] {1L, -1L, 1L, 2L});

        Map<Long, Long[]> partitionSnapshots = new HashMap<>();
        partitionSnapshots.put(2001L, new Long[] {10L, -1L, 20L, 30L});
        partitionSnapshots.put(2002L, new Long[] {15L, -1L, 25L, 35L});
        kvSnapshotTableLeases[1] = new KvSnapshotTableLease(2L, partitionSnapshots);

        return kvSnapshotTableLeases;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"table_id\":1,\"bucket_snapshots\":[1,-1,1,2]}",
            "{\"version\":1,\"table_id\":2,\"partition_snapshots\":["
                    + "{\"partition_id\":2001,\"bucket_snapshots\":[10,-1,20,30]},"
                    + "{\"partition_id\":2002,\"bucket_snapshots\":[15,-1,25,35]}]}"
        };
    }
}
