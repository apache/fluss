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

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.types.DataTypes;

import java.util.Arrays;
import java.util.Collections;

/** Test for {@link SchemaJsonSerde}. */
public class SchemaJsonSerdeTest extends JsonSerdeTestBase<Schema> {

    static final Schema SCHEMA_0 =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .column("c", DataTypes.CHAR(10))
                    .withComment("c is third column")
                    .primaryKey("a", "c")
                    .build();

    static final Schema SCHEMA_1 =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("a", DataTypes.INT()),
                                    new Schema.Column(
                                            "b", DataTypes.STRING(), "b is second column"),
                                    new Schema.Column(
                                            "c", DataTypes.TIMESTAMP(), "c is third column")))
                    .primaryKey(Collections.singletonList("a"))
                    .build();

    static final Schema SCHEMA_2 = Schema.newBuilder().fromSchema(SCHEMA_1).build();

    static final Schema SCHEMA_3 =
            Schema.newBuilder()
                    .column("a", DataTypes.BIGINT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .column("c", DataTypes.TIMESTAMP(6))
                    .withComment("c is third column")
                    .build();

    static final String SCHEMA_JSON_0 =
            "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"a is first column\"},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\"},{\"name\":\"c\",\"data_type\":{\"type\":\"CHAR\",\"nullable\":false,\"length\":10},\"comment\":\"c is third column\"}],\"primary_key\":[\"a\",\"c\"]}";

    static final String SCHEMA_JSON_1 =
            "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false}},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\"},{\"name\":\"c\",\"data_type\":{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":6},\"comment\":\"c is third column\"}],\"primary_key\":[\"a\"]}";

    // shouldn't contain primary_key fields
    static final String SCHEMA_JSON_3 =
            "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"BIGINT\"},\"comment\":\"a is first column\"},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\"},{\"name\":\"c\",\"data_type\":{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":6},\"comment\":\"c is third column\"}]}";

    SchemaJsonSerdeTest() {
        super(SchemaJsonSerde.INSTANCE);
    }

    @Override
    protected Schema[] createObjects() {
        return new Schema[] {SCHEMA_0, SCHEMA_1, SCHEMA_2, SCHEMA_3};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {SCHEMA_JSON_0, SCHEMA_JSON_1, SCHEMA_JSON_1, SCHEMA_JSON_3};
    }
}
