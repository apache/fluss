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

package org.apache.fluss.utils.json;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the lightweight shared-SST parsers in {@link CompletedSnapshotJsonSerde}. */
class KvSnapshotMetadataJsonSerdeTest {

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
                CompletedSnapshotJsonSerde.parseSharedSstRemotePaths(
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
                                CompletedSnapshotJsonSerde.parseSharedSstRemotePaths(
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
                                CompletedSnapshotJsonSerde.parseSharedSstRemotePaths(
                                        json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("kv_file_handle.path");
    }

    @Test
    void parsesSharedSstLocalPathsFromValidMetadata() throws IOException {
        String json =
                "{"
                        + "\"kv_snapshot_handle\":{"
                        + "  \"shared_file_handles\":["
                        + "    {\"local_path\":\"abc-001.sst\",\"size\":1024},"
                        + "    {\"local_path\":\"def-002.sst\",\"size\":2048},"
                        + "    {\"local_path\":\"ghi-003.sst\",\"size\":512}"
                        + "  ]"
                        + "}"
                        + "}";

        Set<String> result =
                CompletedSnapshotJsonSerde.parseSharedSstLocalPaths(
                        json.getBytes(StandardCharsets.UTF_8));

        assertThat(result).containsExactlyInAnyOrder("abc-001.sst", "def-002.sst", "ghi-003.sst");
    }

    @Test
    void returnsEmptySetWhenSharedFileHandlesArrayIsEmpty() throws IOException {
        String json = "{" + "\"kv_snapshot_handle\":{" + "  \"shared_file_handles\":[]" + "}" + "}";

        Set<String> result =
                CompletedSnapshotJsonSerde.parseSharedSstLocalPaths(
                        json.getBytes(StandardCharsets.UTF_8));

        assertThat(result).isEmpty();
    }

    @Test
    void throwsOnMissingKvSnapshotHandle() {
        String json = "{\"other_field\":\"value\"}";

        assertThatThrownBy(
                        () ->
                                CompletedSnapshotJsonSerde.parseSharedSstLocalPaths(
                                        json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("kv_snapshot_handle");
    }

    @Test
    void throwsOnMissingSharedFileHandles() {
        String json = "{\"kv_snapshot_handle\":{\"other_field\":\"value\"}}";

        assertThatThrownBy(
                        () ->
                                CompletedSnapshotJsonSerde.parseSharedSstLocalPaths(
                                        json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("shared_file_handles");
    }

    @Test
    void throwsOnMalformedJson() {
        String json = "not valid json at all";

        assertThatThrownBy(
                        () ->
                                CompletedSnapshotJsonSerde.parseSharedSstLocalPaths(
                                        json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void throwsOnSharedFileHandleMissingLocalPath() {
        String json =
                "{"
                        + "\"kv_snapshot_handle\":{"
                        + "  \"shared_file_handles\":["
                        + "    {\"local_path\":\"valid.sst\",\"size\":100},"
                        + "    {\"size\":200}"
                        + "  ]"
                        + "}"
                        + "}";

        assertThatThrownBy(
                        () ->
                                CompletedSnapshotJsonSerde.parseSharedSstLocalPaths(
                                        json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("local_path");
    }

    @Test
    void throwsOnSharedFileHandleEmptyLocalPath() {
        String json =
                "{"
                        + "\"kv_snapshot_handle\":{"
                        + "  \"shared_file_handles\":["
                        + "    {\"local_path\":\"\",\"size\":300}"
                        + "  ]"
                        + "}"
                        + "}";

        assertThatThrownBy(
                        () ->
                                CompletedSnapshotJsonSerde.parseSharedSstLocalPaths(
                                        json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("local_path");
    }

    @Test
    void deduplicatesRepeatedLocalPaths() throws IOException {
        String json =
                "{"
                        + "\"kv_snapshot_handle\":{"
                        + "  \"shared_file_handles\":["
                        + "    {\"local_path\":\"same.sst\",\"size\":100},"
                        + "    {\"local_path\":\"same.sst\",\"size\":200}"
                        + "  ]"
                        + "}"
                        + "}";

        Set<String> result =
                CompletedSnapshotJsonSerde.parseSharedSstLocalPaths(
                        json.getBytes(StandardCharsets.UTF_8));

        assertThat(result).containsExactly("same.sst");
    }
}
