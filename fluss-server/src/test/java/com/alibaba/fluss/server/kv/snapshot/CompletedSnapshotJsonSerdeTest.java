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

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

import java.util.Arrays;
import java.util.List;

/** Test for {@link com.alibaba.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde}. */
class CompletedSnapshotJsonSerdeTest extends JsonSerdeTestBase<CompletedSnapshot> {

    protected CompletedSnapshotJsonSerdeTest() {
        super(CompletedSnapshotJsonSerde.INSTANCE);
    }

    @Override
    protected CompletedSnapshot[] createObjects() {
        List<KvFileHandleAndLocalPath> sharedFileHandles =
                Arrays.asList(
                        KvFileHandleAndLocalPath.of(
                                new KvFileHandle("oss://bucket/snapshot/shared/t1.sst", 1),
                                "localPath1"),
                        KvFileHandleAndLocalPath.of(
                                new KvFileHandle("oss://bucket/snapshot/shared/t2.sst", 2),
                                "localPath2"));
        List<KvFileHandleAndLocalPath> privateFileHandles =
                Arrays.asList(
                        KvFileHandleAndLocalPath.of(
                                new KvFileHandle("oss://bucket/snapshot/snapshot1/t3", 3),
                                "localPath3"),
                        KvFileHandleAndLocalPath.of(
                                new KvFileHandle("oss://bucket/snapshot/snapshot1/t4", 4),
                                "localPath4"));
        CompletedSnapshot completedSnapshot1 =
                new CompletedSnapshot(
                        new TableBucket(1, 1),
                        1,
                        new FsPath("oss://bucket/snapshot"),
                        new KvSnapshotHandle(sharedFileHandles, privateFileHandles, 5),
                        10);
        CompletedSnapshot completedSnapshot2 =
                new CompletedSnapshot(
                        new TableBucket(1, 10L, 1),
                        1,
                        new FsPath("oss://bucket/snapshot"),
                        new KvSnapshotHandle(sharedFileHandles, privateFileHandles, 5),
                        10);
        return new CompletedSnapshot[] {completedSnapshot1, completedSnapshot2};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,"
                    + "\"table_id\":1,\"bucket_id\":1,"
                    + "\"snapshot_id\":1,"
                    + "\"snapshot_location\":\"oss://bucket/snapshot\","
                    + "\"kv_snapshot_handle\":{"
                    + "\"shared_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/t1.sst\",\"size\":1},\"local_path\":\"localPath1\"},"
                    + "{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/t2.sst\",\"size\":2},\"local_path\":\"localPath2\"}],"
                    + "\"private_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/snapshot1/t3\",\"size\":3},\"local_path\":\"localPath3\"},"
                    + "{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/snapshot1/t4\",\"size\":4},\"local_path\":\"localPath4\"}],"
                    + "\"snapshot_incremental_size\":5},\"log_offset\":10}",
            "{\"version\":1,"
                    + "\"table_id\":1,\"partition_id\":10,\"bucket_id\":1,"
                    + "\"snapshot_id\":1,"
                    + "\"snapshot_location\":\"oss://bucket/snapshot\","
                    + "\"kv_snapshot_handle\":{"
                    + "\"shared_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/t1.sst\",\"size\":1},\"local_path\":\"localPath1\"},"
                    + "{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/t2.sst\",\"size\":2},\"local_path\":\"localPath2\"}],"
                    + "\"private_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/snapshot1/t3\",\"size\":3},\"local_path\":\"localPath3\"},"
                    + "{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/snapshot1/t4\",\"size\":4},\"local_path\":\"localPath4\"}],"
                    + "\"snapshot_incremental_size\":5},\"log_offset\":10}"
        };
    }
}
