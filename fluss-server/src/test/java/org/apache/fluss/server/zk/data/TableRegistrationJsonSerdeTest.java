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

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableDescriptor.TableDistribution;
import com.alibaba.fluss.record.TestData;
import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

import org.apache.fluss.shaded.guava32.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link com.alibaba.fluss.server.zk.data.TableRegistrationJsonSerde}. */
class TableRegistrationJsonSerdeTest extends JsonSerdeTestBase<TableRegistration> {
    TableRegistrationJsonSerdeTest() {
        super(TableRegistrationJsonSerde.INSTANCE);
    }

    @Test
    void testInvalidTableRegistration() {
        // null bucket count
        assertThatThrownBy(
                        () ->
                                new TableRegistration(
                                        1234L,
                                        "first-table",
                                        Arrays.asList("a", "b"),
                                        new TableDistribution(null, Arrays.asList("b", "c")),
                                        Maps.newHashMap(),
                                        Collections.singletonMap("custom-3", "\"300\""),
                                        1735538268L,
                                        1735538268L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bucket count is required for table registration.");

        // null distribution
        assertThatThrownBy(
                        () ->
                                TableRegistration.newTable(
                                        11,
                                        TableDescriptor.builder()
                                                .schema(TestData.DATA1_SCHEMA)
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Table distribution is required for table registration.");
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
                        new TableDistribution(32, Collections.emptyList()),
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
            "{\"version\":1,\"table_id\":1234,\"comment\":\"second-table\",\"bucket_count\":32,\"properties\":{\"option-3\":\"300\"},\"custom_properties\":{},\"created_time\":-1,\"modified_time\":-1}",
        };
    }
}
