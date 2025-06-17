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

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

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
                new LakeTableSnapshot(1, 1L, Collections.emptyMap(), Collections.emptyMap());

        long tableId = 4;
        Map<TableBucket, Long> bucketLogStartOffset = new HashMap<>();
        bucketLogStartOffset.put(new TableBucket(tableId, 1), 1L);
        bucketLogStartOffset.put(new TableBucket(tableId, 2), 2L);
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 1), 3L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2), 4L);

        LakeTableSnapshot lakeTableSnapshot2 =
                new LakeTableSnapshot(2, tableId, bucketLogStartOffset, bucketLogEndOffset);

        tableId = 5;
        bucketLogStartOffset = new HashMap<>();
        bucketLogStartOffset.put(new TableBucket(tableId, 1L, 1), 1L);
        bucketLogStartOffset.put(new TableBucket(tableId, 2L, 1), 2L);

        bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 1), 3L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2L, 1), 4L);

        LakeTableSnapshot lakeTableSnapshot3 =
                new LakeTableSnapshot(3, tableId, bucketLogStartOffset, bucketLogEndOffset);

        return new LakeTableSnapshot[] {
            lakeTableSnapshot1, lakeTableSnapshot2, lakeTableSnapshot3,
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"snapshot_id\":1,\"table_id\":1,\"buckets\":[]}",
            "{\"version\":1,\"snapshot_id\":2,\"table_id\":4,"
                    + "\"buckets\":[{\"bucket_id\":2,\"log_start_offset\":2,\"log_end_offset\":4},"
                    + "{\"bucket_id\":1,\"log_start_offset\":1,\"log_end_offset\":3}]}",
            "{\"version\":1,\"snapshot_id\":3,\"table_id\":5,"
                    + "\"buckets\":[{\"partition_id\":1,\"bucket_id\":1,\"log_start_offset\":1,\"log_end_offset\":3},"
                    + "{\"partition_id\":2,\"bucket_id\":1,\"log_start_offset\":2,\"log_end_offset\":4}]}"
        };
    }
}
