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
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RebalanceTaskZNodeJsonSerde}. */
class RebalanceTaskZNodeJsonSerdeTest {

    @Test
    void testRemoteHandleRoundTrip() throws Exception {
        RebalanceTaskHandle handle =
                new RebalanceTaskHandle(
                        "rebalance-1", new FsPath("file:///remote/rebalance/rebalance-1/id.task"));

        byte[] bytes =
                JsonSerdeUtils.writeValueAsBytes(
                        RebalanceTaskZNodeData.remote(handle),
                        RebalanceTaskZNodeJsonSerde.INSTANCE);
        RebalanceTaskZNodeData decoded =
                JsonSerdeUtils.readValue(bytes, RebalanceTaskZNodeJsonSerde.INSTANCE);

        assertThat(new String(bytes, StandardCharsets.UTF_8))
                .isEqualTo(
                        "{\"version\":3,\"rebalance_id\":\"rebalance-1\","
                                + "\"rebalance_task_path\":\"file:/remote/rebalance/rebalance-1/id.task\"}");
        assertThat(decoded.isInline()).isFalse();
        assertThat(decoded.getRemoteTaskHandle()).isEqualTo(handle);
    }
}
