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

package org.apache.fluss.server.zk.data.lake;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Test for {@link LakeTableJsonSerde}. */
class LakeTableJsonSerdeTest extends JsonSerdeTestBase<LakeTable> {

    LakeTableJsonSerdeTest() {
        super(LakeTableJsonSerde.INSTANCE);
    }

    @Override
    protected LakeTable[] createObjects() {
        // Test case 1: Empty lake snapshots list
        LakeTable lakeTable1 = new LakeTable(Collections.emptyList());

        // Test case 2: Single snapshot metadata with readable offsets
        LakeTable.LakeSnapshotMetadata metadata1 =
                new LakeTable.LakeSnapshotMetadata(
                        1L, new FsPath("/path/to/tiered1"), new FsPath("/path/to/readable1"));
        LakeTable lakeTable2 = new LakeTable(Collections.singletonList(metadata1));

        // Test case 3: Single snapshot metadata without readable offsets
        LakeTable.LakeSnapshotMetadata metadata2 =
                new LakeTable.LakeSnapshotMetadata(2L, new FsPath("/path/to/tiered2"), null);
        LakeTable lakeTable3 = new LakeTable(Collections.singletonList(metadata2));

        // Test case 4: Multiple snapshot metadata
        List<LakeTable.LakeSnapshotMetadata> metadatas = new ArrayList<>();
        metadatas.add(
                new LakeTable.LakeSnapshotMetadata(
                        3L, new FsPath("/path/to/tiered3"), new FsPath("/path/to/readable3")));
        metadatas.add(
                new LakeTable.LakeSnapshotMetadata(
                        4L, new FsPath("/path/to/tiered4"), new FsPath("/path/to/readable4")));
        metadatas.add(new LakeTable.LakeSnapshotMetadata(5L, new FsPath("/path/to/tiered5"), null));
        LakeTable lakeTable4 = new LakeTable(metadatas);

        return new LakeTable[] {lakeTable1, lakeTable2, lakeTable3, lakeTable4};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            // Test case 1: Empty lake snapshots list
            "{\"version\":2,\"lake_snapshots\":[]}",
            // Test case 2: Single snapshot metadata with readable offsets
            "{\"version\":2,\"lake_snapshots\":[{\"snapshot_id\":1,\"tiered_offsets\":\"/path/to/tiered1\",\"readable_offsets\":\"/path/to/readable1\"}]}",
            // Test case 3: Single snapshot metadata without readable offsets
            "{\"version\":2,\"lake_snapshots\":[{\"snapshot_id\":2,\"tiered_offsets\":\"/path/to/tiered2\"}]}",
            // Test case 4: Multiple snapshot metadata
            "{\"version\":2,\"lake_snapshots\":[{\"snapshot_id\":3,\"tiered_offsets\":\"/path/to/tiered3\",\"readable_offsets\":\"/path/to/readable3\"},{\"snapshot_id\":4,\"tiered_offsets\":\"/path/to/tiered4\",\"readable_offsets\":\"/path/to/readable4\"},{\"snapshot_id\":5,\"tiered_offsets\":\"/path/to/tiered5\"}]}"
        };
    }
}
