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

package com.alibaba.fluss.utils.json;

import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.TableDescriptor;

import java.util.Collections;

/** Test for {@link TableDescriptorJsonSerde}. */
public class TableDescriptorJsonSerdeTest extends JsonSerdeTestBase<TableDescriptor> {
    TableDescriptorJsonSerdeTest() {
        super(TableDescriptorJsonSerde.INSTANCE);
    }

    @Override
    protected TableDescriptor[] createObjects() {
        TableDescriptor[] tableDescriptors = new TableDescriptor[2];

        tableDescriptors[0] =
                TableDescriptor.builder()
                        .schema(SchemaJsonSerdeTest.SCHEMA_0)
                        .comment("first table")
                        .partitionedBy("c")
                        .distributedBy(16, "a")
                        .property("option-1", "100")
                        .property("option-2", "200")
                        .customProperties(Collections.singletonMap("custom-1", "\"value-1\""))
                        .build();

        tableDescriptors[1] =
                TableDescriptor.builder()
                        .schema(SchemaJsonSerdeTest.SCHEMA_1)
                        .distributedBy(32)
                        .property("option-3", "300")
                        .property("option-4", "400")
                        .logFormat(LogFormat.INDEXED)
                        .kvFormat(KvFormat.INDEXED)
                        .build();

        return tableDescriptors;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"schema\":"
                    + SchemaJsonSerdeTest.SCHEMA_JSON_0
                    + ",\"comment\":\"first table\",\"partition_key\":[\"c\"],\"bucket_key\":[\"a\"],\"bucket_count\":16,\"properties\":{\"option-2\":\"200\",\"option-1\":\"100\"},"
                    + "\"custom_properties\":{\"custom-1\":\"\\\"value-1\\\"\"}}",
            "{\"version\":1,\"schema\":"
                    + SchemaJsonSerdeTest.SCHEMA_JSON_1
                    + ",\"partition_key\":[],\"bucket_key\":[\"a\"],\"bucket_count\":32,\"properties\":{\"option-3\":\"300\",\"option-4\":\"400\","
                    + "\"table.log.format\":\"INDEXED\",\"table.kv.format\":\"INDEXED\"},\"custom_properties\":{}}"
        };
    }
}
