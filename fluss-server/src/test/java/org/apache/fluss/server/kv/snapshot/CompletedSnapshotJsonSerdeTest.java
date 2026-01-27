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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.json.JsonSerdeTestBase;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link CompletedSnapshotJsonSerde}.
 *
 * <p>This test verifies JSON serialization/deserialization and backward compatibility between V1
 * (absolute paths) and V2 (relative paths) formats.
 */
class CompletedSnapshotJsonSerdeTest extends JsonSerdeTestBase<CompletedSnapshot> {

    protected CompletedSnapshotJsonSerdeTest() {
        super(CompletedSnapshotJsonSerde.INSTANCE);
    }

    @Override
    protected CompletedSnapshot[] createObjects() {
        List<KvFileHandleAndLocalPath> sharedFileHandles =
                Arrays.asList(
                        KvFileHandleAndLocalPath.of(new KvFileHandle("t1.sst", 1), "localPath1"),
                        KvFileHandleAndLocalPath.of(new KvFileHandle("t2.sst", 2), "localPath2"));
        List<KvFileHandleAndLocalPath> privateFileHandles =
                Arrays.asList(
                        KvFileHandleAndLocalPath.of(new KvFileHandle("t3", 3), "localPath3"),
                        KvFileHandleAndLocalPath.of(new KvFileHandle("t4", 4), "localPath4"));
        CompletedSnapshot completedSnapshot1 =
                new CompletedSnapshot(
                        new TableBucket(1, 1),
                        1,
                        new KvSnapshotHandle(sharedFileHandles, privateFileHandles, 5),
                        10);
        CompletedSnapshot completedSnapshot2 =
                new CompletedSnapshot(
                        new TableBucket(1, 10L, 1),
                        1,
                        new KvSnapshotHandle(sharedFileHandles, privateFileHandles, 5),
                        10);
        return new CompletedSnapshot[] {completedSnapshot1, completedSnapshot2};
    }

    @Override
    protected String[] expectedJsons() {
        // V2 format: version=2, uses relative paths
        return new String[] {
            "{\"version\":2,"
                    + "\"table_id\":1,\"bucket_id\":1,"
                    + "\"snapshot_id\":1,"
                    + "\"kv_snapshot_handle\":{"
                    + "\"shared_file_handles\":[{\"kv_file_handle\":{\"path\":\"t1.sst\",\"size\":1},\"local_path\":\"localPath1\"},"
                    + "{\"kv_file_handle\":{\"path\":\"t2.sst\",\"size\":2},\"local_path\":\"localPath2\"}],"
                    + "\"private_file_handles\":[{\"kv_file_handle\":{\"path\":\"t3\",\"size\":3},\"local_path\":\"localPath3\"},"
                    + "{\"kv_file_handle\":{\"path\":\"t4\",\"size\":4},\"local_path\":\"localPath4\"}],"
                    + "\"snapshot_incremental_size\":5},\"log_offset\":10}",
            "{\"version\":2,"
                    + "\"table_id\":1,\"partition_id\":10,\"bucket_id\":1,"
                    + "\"snapshot_id\":1,"
                    + "\"kv_snapshot_handle\":{"
                    + "\"shared_file_handles\":[{\"kv_file_handle\":{\"path\":\"t1.sst\",\"size\":1},\"local_path\":\"localPath1\"},"
                    + "{\"kv_file_handle\":{\"path\":\"t2.sst\",\"size\":2},\"local_path\":\"localPath2\"}],"
                    + "\"private_file_handles\":[{\"kv_file_handle\":{\"path\":\"t3\",\"size\":3},\"local_path\":\"localPath3\"},"
                    + "{\"kv_file_handle\":{\"path\":\"t4\",\"size\":4},\"local_path\":\"localPath4\"}],"
                    + "\"snapshot_incremental_size\":5},\"log_offset\":10}"
        };
    }

    /** Test backward compatibility with V1 format (absolute paths). */
    @Test
    void testV1Compatibility() {
        // V1 format: no version field or version=1, uses absolute paths
        // Test case 1: Non-partitioned table without version field (implicit V1)
        String v1JsonWithoutVersion =
                "{\"table_id\":1,\"bucket_id\":1,"
                        + "\"snapshot_id\":1,"
                        + "\"kv_snapshot_handle\":{"
                        + "\"shared_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/t1.sst\",\"size\":1},\"local_path\":\"localPath1\"},"
                        + "{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/t2.sst\",\"size\":2},\"local_path\":\"localPath2\"}],"
                        + "\"private_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/snapshot1/t3\",\"size\":3},\"local_path\":\"localPath3\"},"
                        + "{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/snapshot1/t4\",\"size\":4},\"local_path\":\"localPath4\"}],"
                        + "\"snapshot_incremental_size\":5},\"log_offset\":10}";

        CompletedSnapshot snapshot1 =
                JsonSerdeUtils.readValue(
                        v1JsonWithoutVersion.getBytes(StandardCharsets.UTF_8),
                        CompletedSnapshotJsonSerde.INSTANCE);

        // Verify the file paths are extracted correctly from absolute paths
        assertThat(snapshot1.getTableBucket()).isEqualTo(new TableBucket(1, 1));
        assertThat(snapshot1.getSnapshotID()).isEqualTo(1);
        assertThat(snapshot1.getLogOffset()).isEqualTo(10);
        KvSnapshotHandle handle1 = snapshot1.getKvSnapshotHandle();
        assertThat(handle1.getSharedKvFileHandles()).hasSize(2);
        assertThat(handle1.getSharedKvFileHandles().get(0).getKvFileHandle().getFilePath())
                .isEqualTo("t1.sst");
        assertThat(handle1.getSharedKvFileHandles().get(1).getKvFileHandle().getFilePath())
                .isEqualTo("t2.sst");
        assertThat(handle1.getPrivateFileHandles()).hasSize(2);
        assertThat(handle1.getPrivateFileHandles().get(0).getKvFileHandle().getFilePath())
                .isEqualTo("t3");
        assertThat(handle1.getPrivateFileHandles().get(1).getKvFileHandle().getFilePath())
                .isEqualTo("t4");
        assertThat(handle1.getIncrementalSize()).isEqualTo(5);

        // Test case 2: Partitioned table with explicit version=1
        String v1JsonWithVersion =
                "{\"version\":1,"
                        + "\"table_id\":1,\"partition_id\":10,\"bucket_id\":1,"
                        + "\"snapshot_id\":1,"
                        + "\"kv_snapshot_handle\":{"
                        + "\"shared_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/t1.sst\",\"size\":1},\"local_path\":\"localPath1\"}],"
                        + "\"private_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/snapshot1/t3\",\"size\":3},\"local_path\":\"localPath3\"}],"
                        + "\"snapshot_incremental_size\":5},\"log_offset\":10}";

        CompletedSnapshot snapshot2 =
                JsonSerdeUtils.readValue(
                        v1JsonWithVersion.getBytes(StandardCharsets.UTF_8),
                        CompletedSnapshotJsonSerde.INSTANCE);

        assertThat(snapshot2.getTableBucket()).isEqualTo(new TableBucket(1, 10L, 1));
        assertThat(snapshot2.getSnapshotID()).isEqualTo(1);
        assertThat(snapshot2.getLogOffset()).isEqualTo(10);
        KvSnapshotHandle handle2 = snapshot2.getKvSnapshotHandle();
        assertThat(handle2.getSharedKvFileHandles()).hasSize(1);
        assertThat(handle2.getSharedKvFileHandles().get(0).getKvFileHandle().getFilePath())
                .isEqualTo("t1.sst");
        assertThat(handle2.getPrivateFileHandles()).hasSize(1);
        assertThat(handle2.getPrivateFileHandles().get(0).getKvFileHandle().getFilePath())
                .isEqualTo("t3");
    }
}
