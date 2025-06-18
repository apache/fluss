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
import com.alibaba.fluss.types.IntType;

/** Test for {@link ColumnJsonSerde}. */
public class ColumnJsonSerdeTest extends JsonSerdeTestBase<Schema.Column> {
    protected ColumnJsonSerdeTest() {
        super(ColumnJsonSerde.INSTANCE);
    }

    @Override
    protected Schema.Column[] createObjects() {
        Schema.Column[] columns = new Schema.Column[3];
        columns[0] = new Schema.Column("a", DataTypes.STRING());
        columns[1] = new Schema.Column("b", DataTypes.INT(), "hello b");
        columns[2] = new Schema.Column("c", new IntType(false), "hello c");
        return columns;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"name\":\"a\",\"data_type\":{\"type\":\"STRING\"}}",
            "{\"name\":\"b\",\"data_type\":{\"type\":\"INTEGER\"},\"comment\":\"hello b\"}",
            "{\"name\":\"c\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"hello c\"}"
        };
    }
}
