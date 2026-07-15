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

import org.apache.fluss.lake.committer.LakeTieringTableState;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.GetLakeSnapshotResponse;
import org.apache.fluss.rpc.messages.PbTableOffsets;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.utils.json.TableBucketOffsets;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the opaque tiering-state passthrough in {@link ServerRpcMessageUtils}, covering the
 * PREPARE read path ({@link ServerRpcMessageUtils#toTableBucketOffsets}) and the GET fill path
 * ({@link ServerRpcMessageUtils#makeGetLakeSnapshotResponse}).
 */
class ServerRpcMessageUtilsTieringStateTest {

    @Test
    void testTieringStatePassthroughRoundTrip() {
        // PREPARE read (toTableBucketOffsets) -> persist -> GET fill (makeGetLakeSnapshotResponse).
        long tableId = 1L;
        byte[] tieringStateJson =
                new LakeTieringTableState(true, Collections.singletonMap(5L, 1704153550000L))
                        .toJsonBytes();

        PbTableOffsets pbTableOffsets = new PbTableOffsets();
        pbTableOffsets.setTableId(tableId);
        pbTableOffsets.setTablePath().setDatabaseName("db").setTableName("t");
        pbTableOffsets.addBucketOffset().setPartitionId(5L).setBucketId(0).setLogEndOffset(100L);
        pbTableOffsets.setTieringStateJson(tieringStateJson);

        TableBucketOffsets offsets = ServerRpcMessageUtils.toTableBucketOffsets(pbTableOffsets);
        assertThat(offsets.getOffsets()).containsEntry(new TableBucket(tableId, 5L, 0), 100L);
        assertThat(offsets.getTieringStateJson()).isEqualTo(tieringStateJson);

        LakeTableSnapshot snapshot =
                new LakeTableSnapshot(9L, offsets.getOffsets(), offsets.getTieringStateJson());
        GetLakeSnapshotResponse response =
                ServerRpcMessageUtils.makeGetLakeSnapshotResponse(tableId, snapshot);
        assertThat(response.getTableId()).isEqualTo(tableId);
        assertThat(response.getSnapshotId()).isEqualTo(9L);
        assertThat(response.hasTieringStateJson()).isTrue();
        assertThat(response.getTieringStateJson()).isEqualTo(tieringStateJson);
    }

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
