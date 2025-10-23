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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Test for {@link LakeTableSnapshotJsonSerde}. */
class LakeTableSnapshotJsonSerdeTest extends JsonSerdeTestBase<LakeTableSnapshot> {

    LakeTableSnapshotJsonSerdeTest() {
        super(LakeTableSnapshotJsonSerde.INSTANCE);
    }

    @Override
    protected LakeTableSnapshot[] createObjects() {
        LakeTableSnapshot lakeTableSnapshot1 =
                new LakeTableSnapshot(
                        1,
                        1L,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        long tableId = 4;
        Map<TableBucket, Long> bucketLogStartOffset = new HashMap<>();
        bucketLogStartOffset.put(new TableBucket(tableId, 1), 1L);
        bucketLogStartOffset.put(new TableBucket(tableId, 2), 2L);
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 1), 3L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2), 4L);
        Map<TableBucket, Long> bucketMaxTimestamp = new HashMap<>();
        bucketMaxTimestamp.put(new TableBucket(tableId, 1), 5L);
        bucketMaxTimestamp.put(new TableBucket(tableId, 2), 6L);

        LakeTableSnapshot lakeTableSnapshot2 =
                new LakeTableSnapshot(
                        2,
                        tableId,
                        bucketLogStartOffset,
                        bucketLogEndOffset,
                        bucketMaxTimestamp,
                        Collections.emptyMap());

        tableId = 5;
        bucketLogStartOffset = new HashMap<>();
        Map<Long, String> partitionNameIdByPartitionId = new HashMap<>();
        partitionNameIdByPartitionId.put(1L, "partition1");
        partitionNameIdByPartitionId.put(2L, "partition2");
        bucketLogStartOffset.put(new TableBucket(tableId, 1L, 1), 1L);
        bucketLogStartOffset.put(new TableBucket(tableId, 2L, 1), 2L);

        bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 1), 3L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2L, 1), 4L);

        bucketMaxTimestamp = new HashMap<>();
        bucketMaxTimestamp.put(new TableBucket(tableId, 1L, 1), 5L);
        bucketMaxTimestamp.put(new TableBucket(tableId, 2L, 1), 6L);

        LakeTableSnapshot lakeTableSnapshot3 =
                new LakeTableSnapshot(
                        3,
                        tableId,
                        bucketLogStartOffset,
                        bucketLogEndOffset,
                        bucketMaxTimestamp,
                        partitionNameIdByPartitionId);

        return new LakeTableSnapshot[] {
            lakeTableSnapshot1, lakeTableSnapshot2, lakeTableSnapshot3,
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"snapshot_id\":1,\"table_id\":1,\"buckets\":[]}",
            "{\"version\":1,\"snapshot_id\":2,\"table_id\":4,"
                    + "\"buckets\":[{\"bucket_id\":2,\"log_start_offset\":2,\"log_end_offset\":4,\"max_timestamp\":6},"
                    + "{\"bucket_id\":1,\"log_start_offset\":1,\"log_end_offset\":3,\"max_timestamp\":5}]}",
            "{\"version\":1,\"snapshot_id\":3,\"table_id\":5,"
                    + "\"buckets\":[{\"partition_id\":1,\"partition_name\":\"partition1\",\"bucket_id\":1,\"log_start_offset\":1,\"log_end_offset\":3,\"max_timestamp\":5},"
                    + "{\"partition_id\":2,\"partition_name\":\"partition2\",\"bucket_id\":1,\"log_start_offset\":2,\"log_end_offset\":4,\"max_timestamp\":6}]}"
        };
    }
}
