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
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshotLegacyJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeTestBase;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LakeTableSnapshotLegacyJsonSerde}. */
class LakeTableSnapshotLegacyJsonSerdeTest extends JsonSerdeTestBase<LakeTableSnapshot> {

    LakeTableSnapshotLegacyJsonSerdeTest() {
        super(LakeTableSnapshotLegacyJsonSerde.INSTANCE);
    }

    @Override
    protected LakeTableSnapshot[] createObjects() {

        long tableId = 4;

        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 1), 3L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2), 4L);

        LakeTableSnapshot lakeTableSnapshot1 = new LakeTableSnapshot(2, bucketLogEndOffset);

        tableId = 5;

        bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 1), 3L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2L, 1), 4L);

        LakeTableSnapshot lakeTableSnapshot2 = new LakeTableSnapshot(3, bucketLogEndOffset);

        return new LakeTableSnapshot[] {
            lakeTableSnapshot1, lakeTableSnapshot2,
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"snapshot_id\":2,\"table_id\":4,"
                    + "\"buckets\":[{\"bucket_id\":2,\"log_end_offset\":4},"
                    + "{\"bucket_id\":1,\"log_end_offset\":3}]}",
            "{\"version\":1,\"snapshot_id\":3,\"table_id\":5,"
                    + "\"buckets\":[{\"partition_id\":1,\"bucket_id\":1,\"log_end_offset\":3},"
                    + "{\"partition_id\":2,\"bucket_id\":1,\"log_end_offset\":4}]}"
        };
    }

    @Test
    void testBackwardCompatibility() {
        // Test that Version 1 format can still be deserialized
        String version1Json1 = "{\"version\":1,\"snapshot_id\":1,\"table_id\":1,\"buckets\":[]}";
        LakeTableSnapshot snapshot1 =
                JsonSerdeUtils.readValue(
                        version1Json1.getBytes(StandardCharsets.UTF_8),
                        LakeTableSnapshotLegacyJsonSerde.INSTANCE);
        assertThat(snapshot1.getSnapshotId()).isEqualTo(1);
        assertThat(snapshot1.getBucketLogEndOffset()).isEmpty();

        String version1Json2 =
                "{\"version\":1,\"snapshot_id\":2,\"table_id\":4,"
                        + "\"buckets\":[{\"bucket_id\":1,\"log_start_offset\":1,\"log_end_offset\":3,\"max_timestamp\":5}]}";
        LakeTableSnapshot snapshot2 =
                JsonSerdeUtils.readValue(
                        version1Json2.getBytes(StandardCharsets.UTF_8),
                        LakeTableSnapshotLegacyJsonSerde.INSTANCE);
        assertThat(snapshot2.getSnapshotId()).isEqualTo(2);
        assertThat(snapshot2.getBucketLogEndOffset()).hasSize(1);

        String version1Json3 =
                "{\"version\":1,\"snapshot_id\":3,\"table_id\":5,"
                        + "\"buckets\":[{\"partition_id\":1,\"partition_name\":\"partition1\",\"bucket_id\":1,\"log_start_offset\":1,\"log_end_offset\":3,\"max_timestamp\":5}]}";
        LakeTableSnapshot snapshot3 =
                JsonSerdeUtils.readValue(
                        version1Json3.getBytes(StandardCharsets.UTF_8),
                        LakeTableSnapshotLegacyJsonSerde.INSTANCE);
        assertThat(snapshot3.getSnapshotId()).isEqualTo(3);
    }
}
