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

import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.HashMap;
import java.util.Map;

/** Test for {@link ServerTagsJsonSerde}. */
public class ServerTagsJsonSerdeTest extends JsonSerdeTestBase<ServerTags> {

    ServerTagsJsonSerdeTest() {
        super(ServerTagsJsonSerde.INSTANCE);
    }

    @Override
    protected ServerTags[] createObjects() {
        Map<Integer, ServerTag> serverTags = new HashMap<>();
        serverTags.put(0, ServerTag.PERMANENT_OFFLINE);
        serverTags.put(1, ServerTag.TEMPORARY_OFFLINE);

        Map<Integer, ServerTag> serverTags2 = new HashMap<>();

        return new ServerTags[] {new ServerTags(serverTags), new ServerTags(serverTags2)};
    }

    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"server_tags\":{\"0\":0,\"1\":1}}",
            "{\"version\":1,\"server_tags\":{}}"
        };
    }
}
