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
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshotJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeTestBase;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LakeTableSnapshotJsonSerde}. */
class LakeTableSnapshotJsonSerdeTest extends JsonSerdeTestBase<LakeTableSnapshot> {

    LakeTableSnapshotJsonSerdeTest() {
        super(LakeTableSnapshotJsonSerde.INSTANCE);
    }

    @Override
    protected LakeTableSnapshot[] createObjects() {
        // Test case 1: Empty snapshot
        LakeTableSnapshot lakeTableSnapshot1 = new LakeTableSnapshot(1L, Collections.emptyMap());

        // Test case 2: Non-partition table with consecutive bucket ids (0, 1, 2)
        long tableId = 4;
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 0), 100L);
        bucketLogEndOffset.put(new TableBucket(tableId, 1), 200L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2), 300L);
        LakeTableSnapshot lakeTableSnapshot2 = new LakeTableSnapshot(2, bucketLogEndOffset);

        // Test case 3: Non-partition table with missing bucket ids (0, 2, 4 - missing 1 and 3)
        tableId = 5;
        bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 0), 100L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2), 300L);
        bucketLogEndOffset.put(new TableBucket(tableId, 4), 500L);
        LakeTableSnapshot lakeTableSnapshot3 = new LakeTableSnapshot(3, bucketLogEndOffset);

        // Test case 4: Partition table with consecutive bucket ids
        tableId = 6;
        bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 0), 100L);
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 1), 200L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2L, 0), 300L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2L, 1), 400L);
        LakeTableSnapshot lakeTableSnapshot4 = new LakeTableSnapshot(4, bucketLogEndOffset);

        // Test case 5: Partition table with missing bucket ids
        tableId = 7;
        bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 0), 100L);
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 2), 300L); // missing bucket 1
        bucketLogEndOffset.put(new TableBucket(tableId, 2L, 1), 400L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2L, 3), 600L); // missing bucket 0 and 2
        LakeTableSnapshot lakeTableSnapshot5 = new LakeTableSnapshot(5, bucketLogEndOffset);

        return new LakeTableSnapshot[] {
            lakeTableSnapshot1,
            lakeTableSnapshot2,
            lakeTableSnapshot3,
            lakeTableSnapshot4,
            lakeTableSnapshot5,
        };
    }

    @Override
    protected String[] expectedJsons() {
        // Version 2 format (uses different property keys):
        // - Non-partition table: "bucket_offsets": [100, 200, 300], array index = bucket id,
        //   value = log_end_offset. Missing buckets are filled with -1.
        // - Partition table: "partition_bucket_offsets": {"1": [100, 200], "2": [300, 400]},
        //   key = partition id, array index = bucket id, value = log_end_offset. Missing buckets
        //   are filled with -1.
        return new String[] {
            // Test case 1: Empty snapshot
            "{\"version\":2,\"snapshot_id\":1}",
            // Test case 2: Non-partition table with consecutive bucket ids [0, 1, 2]
            "{\"version\":2,\"snapshot_id\":2,\"table_id\":4,\"bucket_offsets\":[100,200,300]}",
            // Test case 3: Non-partition table with missing bucket ids [0, -1, 2, -1, 4]
            "{\"version\":2,\"snapshot_id\":3,\"table_id\":5,\"bucket_offsets\":[100,-1,300,-1,500]}",
            // Test case 4: Partition table with consecutive bucket ids
            "{\"version\":2,\"snapshot_id\":4,\"table_id\":6,"
                    + "\"partition_bucket_offsets\":{\"1\":[100,200],\"2\":[300,400]}}",
            // Test case 5: Partition table with missing bucket ids
            "{\"version\":2,\"snapshot_id\":5,\"table_id\":7,"
                    + "\"partition_bucket_offsets\":{\"1\":[100,-1,300],\"2\":[-1,400,-1,600]}}"
        };
    }

    @Test
    void testBackwardCompatibility() {
        // Test that Version 1 format can still be deserialized
        String version1Json1 = "{\"version\":1,\"snapshot_id\":1,\"table_id\":1,\"buckets\":[]}";
        LakeTableSnapshot snapshot1 =
                JsonSerdeUtils.readValue(
                        version1Json1.getBytes(StandardCharsets.UTF_8),
                        LakeTableSnapshotJsonSerde.INSTANCE);
        assertThat(snapshot1.getSnapshotId()).isEqualTo(1);
        assertThat(snapshot1.getBucketLogEndOffset()).isEmpty();

        String version1Json2 =
                "{\"version\":1,\"snapshot_id\":2,\"table_id\":4,"
                        + "\"buckets\":[{\"bucket_id\":1,\"log_start_offset\":1,\"log_end_offset\":3,\"max_timestamp\":5}]}";
        LakeTableSnapshot snapshot2 =
                JsonSerdeUtils.readValue(
                        version1Json2.getBytes(StandardCharsets.UTF_8),
                        LakeTableSnapshotJsonSerde.INSTANCE);
        assertThat(snapshot2.getSnapshotId()).isEqualTo(2);
        assertThat(snapshot2.getBucketLogEndOffset()).hasSize(1);

        String version1Json3 =
                "{\"version\":1,\"snapshot_id\":3,\"table_id\":5,"
                        + "\"buckets\":[{\"partition_id\":1,\"partition_name\":\"partition1\",\"bucket_id\":1,\"log_start_offset\":1,\"log_end_offset\":3,\"max_timestamp\":5}]}";
        LakeTableSnapshot snapshot3 =
                JsonSerdeUtils.readValue(
                        version1Json3.getBytes(StandardCharsets.UTF_8),
                        LakeTableSnapshotJsonSerde.INSTANCE);
        assertThat(snapshot3.getSnapshotId()).isEqualTo(3);
    }
}
