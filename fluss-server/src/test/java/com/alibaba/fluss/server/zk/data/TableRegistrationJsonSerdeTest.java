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

import com.alibaba.fluss.metadata.TableDescriptor.TableDistribution;
import com.alibaba.fluss.shaded.guava32.com.google.common.collect.Maps;
import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

import java.util.Arrays;
import java.util.Collections;

/** Test for {@link com.alibaba.fluss.server.zk.data.TableRegistrationJsonSerde}. */
class TableRegistrationJsonSerdeTest extends JsonSerdeTestBase<TableRegistration> {
    TableRegistrationJsonSerdeTest() {
        super(TableRegistrationJsonSerde.INSTANCE);
    }

    @Override
    protected TableRegistration[] createObjects() {
        TableRegistration[] tableRegistrations = new TableRegistration[2];

        tableRegistrations[0] =
                new TableRegistration(
                        1234L,
                        "first-table",
                        Arrays.asList("a", "b"),
                        new TableDistribution(16, Arrays.asList("b", "c")),
                        Maps.newHashMap(),
                        Collections.singletonMap("custom-3", "\"300\""),
                        1735538268L,
                        1735538268L);

        tableRegistrations[1] =
                new TableRegistration(
                        1234L,
                        "second-table",
                        Collections.emptyList(),
                        null,
                        Collections.singletonMap("option-3", "300"),
                        Maps.newHashMap(),
                        -1,
                        -1);

        return tableRegistrations;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"table_id\":1234,\"comment\":\"first-table\",\"partition_key\":[\"a\",\"b\"],"
                    + "\"bucket_key\":[\"b\",\"c\"],\"bucket_count\":16,\"properties\":{},\"custom_properties\":{\"custom-3\":\"\\\"300\\\"\"},\"created_time\":1735538268,\"modified_time\":1735538268}",
            "{\"version\":1,\"table_id\":1234,\"comment\":\"second-table\",\"partition_key\":[],\"properties\":{\"option-3\":\"300\"},\"custom_properties\":{},\"created_time\":-1,\"modified_time\":-1}",
        };
    }
}
