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

package org.apache.fluss.utils.json;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ColumnJsonSerde}. */
public class ColumnJsonSerdeTest extends JsonSerdeTestBase<Schema.Column> {
    protected ColumnJsonSerdeTest() {
        super(ColumnJsonSerde.INSTANCE);
    }

    @Override
    protected Schema.Column[] createObjects() {
        Schema.Column[] columns = new Schema.Column[5];
        columns[0] = new Schema.Column("a", DataTypes.STRING());
        columns[1] = new Schema.Column("b", DataTypes.INT(), "hello b");
        columns[2] = new Schema.Column("c", new IntType(false), "hello c");
        columns[3] = new Schema.Column("d", new IntType(false), "hello c", (short) 2);
        columns[4] =
                new Schema.Column(
                        "e",
                        new RowType(
                                true,
                                Arrays.asList(
                                        DataTypes.FIELD("f", DataTypes.STRING()),
                                        DataTypes.FIELD("g", DataTypes.STRING(), 1))),
                        "hello c",
                        (short) 2);
        return columns;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"name\":\"a\",\"data_type\":{\"type\":\"STRING\"},\"id\":-1}",
            "{\"name\":\"b\",\"data_type\":{\"type\":\"INTEGER\"},\"comment\":\"hello b\",\"id\":-1}",
            "{\"name\":\"c\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"hello c\",\"id\":-1}",
            "{\"name\":\"d\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"hello c\",\"id\":2}",
            "{\"name\":\"e\",\"data_type\":{\"type\":\"ROW\",\"fields\":[{\"name\":\"f\",\"field_type\":{\"type\":\"STRING\"},\"field_id\":-1},{\"name\":\"g\",\"field_type\":{\"type\":\"STRING\"},\"field_id\":1}]},\"comment\":\"hello c\",\"id\":2}"
        };
    }

    @Test
    void testDeserializeCompatibility() throws IOException {
        String[] jsonWithoutColumnId = {
            "{\"name\":\"a\",\"data_type\":{\"type\":\"STRING\"}}",
            "{\"name\":\"b\",\"data_type\":{\"type\":\"INTEGER\"},\"comment\":\"hello b\"}",
            "{\"name\":\"c\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"hello c\"}"
        };
        Schema.Column[] columns = new Schema.Column[3];
        columns[0] = new Schema.Column("a", DataTypes.STRING());
        columns[1] = new Schema.Column("b", DataTypes.INT(), "hello b");
        columns[2] = new Schema.Column("c", new IntType(false), "hello c");
        for (int i = 0; i < jsonWithoutColumnId.length; i++) {
            Schema.Column column =
                    JsonSerdeUtils.readValue(
                            jsonWithoutColumnId[i].getBytes(StandardCharsets.UTF_8),
                            ColumnJsonSerde.INSTANCE);
            assertThat(column).isEqualTo(columns[i]);
        }
    }
}
