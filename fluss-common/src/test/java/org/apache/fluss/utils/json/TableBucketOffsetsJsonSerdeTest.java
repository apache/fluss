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

package org.apache.fluss.utils.json;

import org.apache.fluss.lake.committer.LakeTieringTableState;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TableBucketOffsetsJsonSerde}. */
class TableBucketOffsetsJsonSerdeTest extends JsonSerdeTestBase<TableBucketOffsets> {

    public TableBucketOffsetsJsonSerdeTest() {
        super(TableBucketOffsetsJsonSerde.INSTANCE);
    }

    @Override
    protected TableBucketOffsets[] createObjects() {
        // Test case 1: Empty offsets
        TableBucketOffsets tableBucketOffsets1 = new TableBucketOffsets(1L, Collections.emptyMap());

        // Test case 2: Non-partition table with consecutive bucket ids (0, 1, 2)
        long tableId = 4;
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 0), 100L);
        bucketLogEndOffset.put(new TableBucket(tableId, 1), 200L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2), 300L);
        TableBucketOffsets tableBucketOffsets2 =
                new TableBucketOffsets(tableId, bucketLogEndOffset);

        // Test case 3: Non-partition table with missing bucket ids (0, 2, 4 - missing 1 and 3)
        tableId = 5;
        bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 0), 100L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2), 300L);
        bucketLogEndOffset.put(new TableBucket(tableId, 4), 500L);
        TableBucketOffsets tableBucketOffsets3 =
                new TableBucketOffsets(tableId, bucketLogEndOffset);

        // Test case 4: Partition table with consecutive bucket ids
        tableId = 6;
        bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 0), 100L);
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 1), 200L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2L, 0), 300L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2L, 1), 400L);
        TableBucketOffsets tableBucketOffsets4 =
                new TableBucketOffsets(tableId, bucketLogEndOffset);

        // Test case 5: Partition table with missing bucket ids
        tableId = 7;
        bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 0), 100L);
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 2), 300L); // missing bucket 1
        bucketLogEndOffset.put(new TableBucket(tableId, 2L, 1), 400L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2L, 3), 600L); // missing bucket 0 and 2
        TableBucketOffsets tableBucketOffsets5 =
                new TableBucketOffsets(tableId, bucketLogEndOffset);

        // Test case 6: Partition table with an opaque tiering-state payload
        tableId = 8;
        bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 5L, 0), 100L);
        bucketLogEndOffset.put(new TableBucket(tableId, 5L, 1), 230L);
        bucketLogEndOffset.put(new TableBucket(tableId, 4L, 0), 420L);
        Map<Long, Long> partitionUpdateTimes = new HashMap<>();
        partitionUpdateTimes.put(5L, 1704153550000L);
        byte[] tieringStateJson =
                new LakeTieringTableState(true, partitionUpdateTimes).toJsonBytes();
        TableBucketOffsets tableBucketOffsets6 =
                new TableBucketOffsets(tableId, bucketLogEndOffset, tieringStateJson);

        return new TableBucketOffsets[] {
            tableBucketOffsets1,
            tableBucketOffsets2,
            tableBucketOffsets3,
            tableBucketOffsets4,
            tableBucketOffsets5,
            tableBucketOffsets6,
        };
    }

    @Override
    protected String[] expectedJsons() {
        // Format:
        // - Non-partition table: "bucket_offsets": [100, 200, 300], array index = bucket id,
        //   value = offset. Missing buckets are filled with -1.
        // - Partition table: "partition_offsets": [{"partition_id": 1, "bucket_offsets": [100,
        // 200]}, ...],
        //   array index in bucket_offsets = bucket id, value = offset. Missing buckets are filled
        // with -1.
        return new String[] {
            // Test case 1: Empty offsets
            "{\"version\":1,\"table_id\":1}",
            // Test case 2: Non-partition table with consecutive bucket ids [0, 1, 2]
            "{\"version\":1,\"table_id\":4,\"bucket_offsets\":[100,200,300]}",
            // Test case 3: Non-partition table with missing bucket ids [0, -1, 2, -1, 4]
            "{\"version\":1,\"table_id\":5,\"bucket_offsets\":[100,-1,300,-1,500]}",
            // Test case 4: Partition table with consecutive bucket ids
            "{\"version\":1,\"table_id\":6,\"partition_offsets\":[{\"partition_id\":1,\"bucket_offsets\":[100,200]},{\"partition_id\":2,\"bucket_offsets\":[300,400]}]}",
            // Test case 5: Partition table with missing bucket ids
            "{\"version\":1,\"table_id\":7,\"partition_offsets\":[{\"partition_id\":1,\"bucket_offsets\":[100,-1,300]},{\"partition_id\":2,\"bucket_offsets\":[-1,400,-1,600]}]}",
            // Test case 6: Partition table with an opaque tiering-state payload
            "{\"version\":1,\"table_id\":8,\"partition_offsets\":[{\"partition_id\":4,\"bucket_offsets\":[420]},{\"partition_id\":5,\"bucket_offsets\":[100,230]}],\"tiering_state\":{\"version\":1,\"partition_done_initialized\":true,\"partition_update_times\":{\"5\":1704153550000}}}"
        };
    }

    /**
     * Test that a malformed (non-JSON) or non-object tiering-state payload is rejected on
     * serialization, so a bad payload never corrupts the persisted offsets file. The happy path
     * (with/without tiering state, exact JSON and round-trip) is covered by {@link
     * #createObjects()} / {@link #expectedJsons()}.
     */
    @Test
    void testSerializeRejectsBadTieringStateJson() {
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, 1L, 0), 100L);

        assertThatThrownBy(
                        () ->
                                new TableBucketOffsets(
                                                1L,
                                                offsets,
                                                "{not-json".getBytes(StandardCharsets.UTF_8))
                                        .toJsonBytes())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid JSON");

        assertThatThrownBy(
                        () ->
                                new TableBucketOffsets(
                                                1L,
                                                offsets,
                                                "[1,2,3]".getBytes(StandardCharsets.UTF_8))
                                        .toJsonBytes())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a JSON object");
    }
}
