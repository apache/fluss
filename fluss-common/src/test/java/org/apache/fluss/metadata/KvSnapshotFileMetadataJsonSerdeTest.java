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

package org.apache.fluss.metadata;

import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.Collections;

/** Compatibility test for {@link KvSnapshotFileMetadataJsonSerde}. */
class KvSnapshotFileMetadataJsonSerdeTest extends JsonSerdeTestBase<KvSnapshotFileMetadata> {

    static final String GOLDEN_JSON =
            "{\"version\":1,"
                    + "\"table_id\":1,\"partition_id\":10,\"bucket_id\":1,"
                    + "\"snapshot_id\":1,"
                    + "\"snapshot_location\":\"oss://bucket/snapshot\","
                    + "\"kv_snapshot_handle\":{"
                    + "\"shared_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/t1.sst\",\"size\":1},\"local_path\":\"localPath1\"}],"
                    + "\"private_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/snapshot1/t2\",\"size\":2},\"local_path\":\"localPath2\"}],"
                    + "\"snapshot_incremental_size\":3},\"log_offset\":10,\"row_count\":1234,"
                    + "\"auto_inc_id_range\":[{\"column_id\":2,\"start\":10000,\"end\":20000}]}";

    KvSnapshotFileMetadataJsonSerdeTest() {
        super(KvSnapshotFileMetadataJsonSerde.INSTANCE);
    }

    @Override
    protected KvSnapshotFileMetadata[] createObjects() {
        return new KvSnapshotFileMetadata[] {
            new KvSnapshotFileMetadata(
                    new TableBucket(1, 10L, 1),
                    1,
                    "oss://bucket/snapshot",
                    Collections.singletonList(
                            new KvSnapshotFileMetadata.FileHandle(
                                    "oss://bucket/snapshot/shared/t1.sst", 1, "localPath1")),
                    Collections.singletonList(
                            new KvSnapshotFileMetadata.FileHandle(
                                    "oss://bucket/snapshot/snapshot1/t2", 2, "localPath2")),
                    3,
                    10,
                    1234L,
                    Collections.singletonList(
                            new KvSnapshotFileMetadata.AutoIncrementRange(2, 10000, 20000)))
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {GOLDEN_JSON};
    }
}
