/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

/** Test for {@link com.alibaba.fluss.server.zk.data.BucketSnapshotJsonSerde}. */
class BucketSnapshotJsonSerdeTest extends JsonSerdeTestBase<BucketSnapshot> {

    BucketSnapshotJsonSerdeTest() {
        super(BucketSnapshotJsonSerde.INSTANCE);
    }

    @Override
    protected BucketSnapshot[] createObjects() {
        return new BucketSnapshot[] {
            new BucketSnapshot(-1, -1, "oss://test/a1"),
            new BucketSnapshot(1L, 1002L, "oss://test/a2"),
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"snapshot_id\":-1,\"log_offset\":-1,\"metadata_path\":\"oss://test/a1\"}",
            "{\"version\":1,\"snapshot_id\":1,\"log_offset\":1002,\"metadata_path\":\"oss://test/a2\"}"
        };
    }
}
