/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.utils;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.GetLakeSnapshotResponse;
import org.apache.fluss.rpc.messages.PbTableOffsets;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link ServerRpcMessageUtils} omits the tiering state when absent (PREPARE read yields
 * {@code null}, GET fill leaves the field unset). The present-state passthrough is covered
 * end-to-end by {@code CommitLakeTableSnapshotITCase}.
 */
class ServerRpcMessageUtilsTieringStateTest {

    @Test
    void testAbsentTieringState() {
        // PREPARE read without tiering state -> null.
        PbTableOffsets pbTableOffsets = new PbTableOffsets();
        pbTableOffsets.setTableId(2L);
        pbTableOffsets.setTablePath().setDatabaseName("db").setTableName("t");
        pbTableOffsets.addBucketOffset().setBucketId(0).setLogEndOffset(100L);
        assertThat(ServerRpcMessageUtils.toTableBucketOffsets(pbTableOffsets).getTieringStateJson())
                .isNull();

        // GET fill with a null tiering state -> response omits it.
        Map<TableBucket, Long> bucketOffsets = new HashMap<>();
        bucketOffsets.put(new TableBucket(2L, 0), 100L);
        GetLakeSnapshotResponse response =
                ServerRpcMessageUtils.makeGetLakeSnapshotResponse(
                        2L, new LakeTableSnapshot(9L, bucketOffsets, null));
        assertThat(response.hasTieringStateJson()).isFalse();
    }
}
