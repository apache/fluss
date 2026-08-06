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

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.json.JsonSerdeTestBase;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.server.zk.data.RemoteLogManifestHandleJsonSerde}. */
public class RemoteLogManifestHandleJsonSerdeTest
        extends JsonSerdeTestBase<RemoteLogManifestHandle> {
    RemoteLogManifestHandleJsonSerdeTest() {
        super(RemoteLogManifestHandleJsonSerde.INSTANCE);
    }

    @Override
    protected RemoteLogManifestHandle[] createObjects() {
        return new RemoteLogManifestHandle[] {
            new RemoteLogManifestHandle(
                    new FsPath(
                            "oss://test/log/testDb/testTable_150001/0/847532e6-1fec-4d7a-9b17-ce28223a6e72.manifest"),
                    100L),
            RemoteLogManifestHandle.v2(
                    new FsPath("oss://test/log/db/table/0/v2.manifest"), 8L, 5L, 20L),
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"remote_log_manifest_path\":\"oss://test/log/testDb/testTable_150001/0/"
                    + "847532e6-1fec-4d7a-9b17-ce28223a6e72.manifest\",\"remote_log_end_offset\":100}",
            "{\"version\":2,\"remote_log_manifest_path\":\"oss://test/log/db/table/0/v2.manifest\","
                    + "\"remote_log_end_offset\":20,\"manifest_generation\":8,"
                    + "\"remote_log_start_offset\":5}"
        };
    }

    @Test
    void testStrictVersionDispatch() {
        assertInvalid("{\"remote_log_manifest_path\":\"x\",\"remote_log_end_offset\":10}");
        assertInvalid(
                "{\"version\":1,\"remote_log_manifest_path\":\"x\",\"remote_log_end_offset\":10,"
                        + "\"manifest_generation\":1}");
        assertInvalid(
                "{\"version\":2,\"remote_log_manifest_path\":\"x\",\"remote_log_end_offset\":10,"
                        + "\"manifest_generation\":1}");
        assertInvalid(
                "{\"version\":3,\"remote_log_manifest_path\":\"x\",\"remote_log_end_offset\":10}");
    }

    private static void assertInvalid(String json) {
        assertThatThrownBy(
                        () ->
                                JsonSerdeUtils.readValue(
                                        json.getBytes(StandardCharsets.UTF_8),
                                        RemoteLogManifestHandleJsonSerde.INSTANCE))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
