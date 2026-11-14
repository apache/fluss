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

package org.apache.fluss.flink.action.orphan.build;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the lightweight shared-SST parsers in {@link KvSnapshotMetadataReader}. */
class KvSnapshotMetadataReaderTest {

    @Test
    void parsesRemotePathsIndependentlyOfLocalPaths() throws IOException {
        String json =
                "{"
                        + "\"kv_snapshot_handle\":{"
                        + "  \"shared_file_handles\":["
                        + "    {\"kv_file_handle\":{\"path\":\"oss://bucket/kv/db/t-7/0/shared/remote-a\",\"size\":100},\"local_path\":\"000001.sst\"},"
                        + "    {\"kv_file_handle\":{\"path\":\"oss://bucket/kv/db/t-7/0/shared/remote-b\",\"size\":200},\"local_path\":\"000002.sst\"}"
                        + "  ]"
                        + "}"
                        + "}";

        Set<String> result =
                KvSnapshotMetadataReader.parseSharedSstRemotePaths(
                        json.getBytes(StandardCharsets.UTF_8));

        assertThat(result)
                .containsExactlyInAnyOrder(
                        "oss://bucket/kv/db/t-7/0/shared/remote-a",
                        "oss://bucket/kv/db/t-7/0/shared/remote-b");
    }

    @Test
    void remotePathParserFailsClosedWhenRemoteHandleIsMissing() {
        String json =
                "{\"kv_snapshot_handle\":{\"shared_file_handles\":["
                        + "{\"local_path\":\"000001.sst\"}]}}";

        assertThatThrownBy(
                        () ->
                                KvSnapshotMetadataReader.parseSharedSstRemotePaths(
                                        json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("kv_file_handle");
    }

    @Test
    void remotePathParserFailsClosedWhenRemotePathIsEmpty() {
        String json =
                "{\"kv_snapshot_handle\":{\"shared_file_handles\":["
                        + "{\"kv_file_handle\":{\"path\":\"\",\"size\":100},\"local_path\":\"000001.sst\"}]}}";

        assertThatThrownBy(
                        () ->
                                KvSnapshotMetadataReader.parseSharedSstRemotePaths(
                                        json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("kv_file_handle.path");
    }
}
