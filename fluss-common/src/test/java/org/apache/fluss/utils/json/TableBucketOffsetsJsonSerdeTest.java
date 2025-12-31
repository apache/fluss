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

import org.apache.fluss.metadata.TableBucket;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

        return new TableBucketOffsets[] {
            tableBucketOffsets1,
            tableBucketOffsets2,
            tableBucketOffsets3,
            tableBucketOffsets4,
            tableBucketOffsets5,
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
            "{\"version\":1,\"table_id\":7,\"partition_offsets\":[{\"partition_id\":1,\"bucket_offsets\":[100,-1,300]},{\"partition_id\":2,\"bucket_offsets\":[-1,400,-1,600]}]}"
        };
    }
}
