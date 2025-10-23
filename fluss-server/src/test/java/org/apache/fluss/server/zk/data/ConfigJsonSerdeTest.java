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

import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.HashMap;
import java.util.Map;

/** Tests for {@link ConfigJsonSerde}. */
public class ConfigJsonSerdeTest extends JsonSerdeTestBase<Map<String, String>> {

    public ConfigJsonSerdeTest() {
        super(ConfigJsonSerde.INSTANCE);
    }

    @Override
    protected Map<String, String>[] createObjects() {
        Map<String, String>[] maps = new Map[2];
        Map<String, String> map = new HashMap<>();
        map.put("datalake.format", "value1");
        map.put("key2", null);
        map.put("key3", "value3");
        maps[0] = map;
        maps[1] = new HashMap<>();
        return maps;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"configs\":{\"datalake.format\":\"value1\",\"key2\":null,\"key3\":\"value3\"}}",
            "{\"version\":1,\"configs\":{}}"
        };
    }
}
