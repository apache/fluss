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

import com.alibaba.fluss.shaded.guava32.com.google.common.collect.Maps;
import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

import java.util.Collections;

/** Test for {@link DatabaseRegistrationJsonSerde}. */
public class DatabaseRegistrationJsonSerdeTest extends JsonSerdeTestBase<DatabaseRegistration> {
    DatabaseRegistrationJsonSerdeTest() {
        super(DatabaseRegistrationJsonSerde.INSTANCE);
    }

    @Override
    protected DatabaseRegistration[] createObjects() {
        DatabaseRegistration[] databaseRegistrations = new DatabaseRegistration[2];

        databaseRegistrations[0] =
                new DatabaseRegistration(
                        null,
                        Collections.singletonMap("option-3", "300"),
                        1735538268L,
                        1735538268L);

        databaseRegistrations[1] =
                new DatabaseRegistration("second-table", Maps.newHashMap(), -1, -1);

        return databaseRegistrations;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"custom_properties\":{\"option-3\":\"300\"},\"created_time\":1735538268,\"modified_time\":1735538268}",
            "{\"version\":1,\"comment\":\"second-table\",\"custom_properties\":{},\"created_time\":-1,\"modified_time\":-1}",
        };
    }
}
