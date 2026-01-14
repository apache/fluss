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

import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DataTypeJsonSerde}. */
public class DataTypeJsonSerdeTest extends JsonSerdeTestBase<DataType> {

    DataTypeJsonSerdeTest() {
        super(DataTypeJsonSerde.INSTANCE);
    }

    @Override
    protected void assertEquals(DataType actual, DataType expected) {
        // compare with field_id.
        assertThat(DataTypeChecks.equalsWithFieldId(actual, expected)).isTrue();
    }

    @Override
    protected DataType[] createObjects() {
        final List<DataType> types =
                Arrays.asList(
                        new BooleanType(),
                        new TinyIntType(),
                        new SmallIntType(),
                        new IntType(),
                        new BigIntType(),
                        new FloatType(),
                        new DoubleType(),
                        new DecimalType(10),
                        new DecimalType(15, 5),
                        new CharType(),
                        new CharType(5),
                        new StringType(),
                        new BinaryType(),
                        new BinaryType(100),
                        new BytesType(),
                        new DateType(),
                        new TimeType(),
                        new TimeType(3),
                        new TimestampType(),
                        new TimestampType(3),
                        new LocalZonedTimestampType(),
                        new LocalZonedTimestampType(3),
                        new ArrayType(new IntType(false)),
                        new MapType(new BigIntType(false), new IntType(false)),
                        new RowType(
                                true,
                                Arrays.asList(
                                        DataTypes.FIELD("f0", new BigIntType(), null),
                                        DataTypes.FIELD("f1", new IntType(false), null, 1),
                                        DataTypes.FIELD("f2", new StringType(), null, 2))));

        final List<DataType> allTypes = new ArrayList<>();
        // consider nullable
        for (DataType type : types) {
            allTypes.add(type.copy(true));
            allTypes.add(type.copy(false));
        }
        return allTypes.toArray(new DataType[0]);
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"type\":\"BOOLEAN\"}",
            "{\"type\":\"BOOLEAN\",\"nullable\":false}",
            "{\"type\":\"TINYINT\"}",
            "{\"type\":\"TINYINT\",\"nullable\":false}",
            "{\"type\":\"SMALLINT\"}",
            "{\"type\":\"SMALLINT\",\"nullable\":false}",
            "{\"type\":\"INTEGER\"}",
            "{\"type\":\"INTEGER\",\"nullable\":false}",
            "{\"type\":\"BIGINT\"}",
            "{\"type\":\"BIGINT\",\"nullable\":false}",
            "{\"type\":\"FLOAT\"}",
            "{\"type\":\"FLOAT\",\"nullable\":false}",
            "{\"type\":\"DOUBLE\"}",
            "{\"type\":\"DOUBLE\",\"nullable\":false}",
            "{\"type\":\"DECIMAL\",\"precision\":10,\"scale\":0}",
            "{\"type\":\"DECIMAL\",\"nullable\":false,\"precision\":10,\"scale\":0}",
            "{\"type\":\"DECIMAL\",\"precision\":15,\"scale\":5}",
            "{\"type\":\"DECIMAL\",\"nullable\":false,\"precision\":15,\"scale\":5}",
            "{\"type\":\"CHAR\",\"length\":1}",
            "{\"type\":\"CHAR\",\"nullable\":false,\"length\":1}",
            "{\"type\":\"CHAR\",\"length\":5}",
            "{\"type\":\"CHAR\",\"nullable\":false,\"length\":5}",
            "{\"type\":\"STRING\"}",
            "{\"type\":\"STRING\",\"nullable\":false}",
            "{\"type\":\"BINARY\",\"length\":1}",
            "{\"type\":\"BINARY\",\"nullable\":false,\"length\":1}",
            "{\"type\":\"BINARY\",\"length\":100}",
            "{\"type\":\"BINARY\",\"nullable\":false,\"length\":100}",
            "{\"type\":\"BYTES\"}",
            "{\"type\":\"BYTES\",\"nullable\":false}",
            "{\"type\":\"DATE\"}",
            "{\"type\":\"DATE\",\"nullable\":false}",
            "{\"type\":\"TIME_WITHOUT_TIME_ZONE\",\"precision\":0}",
            "{\"type\":\"TIME_WITHOUT_TIME_ZONE\",\"nullable\":false,\"precision\":0}",
            "{\"type\":\"TIME_WITHOUT_TIME_ZONE\",\"precision\":3}",
            "{\"type\":\"TIME_WITHOUT_TIME_ZONE\",\"nullable\":false,\"precision\":3}",
            "{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":6}",
            "{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"nullable\":false,\"precision\":6}",
            "{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":3}",
            "{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"nullable\":false,\"precision\":3}",
            "{\"type\":\"TIMESTAMP_WITH_LOCAL_TIME_ZONE\",\"precision\":6}",
            "{\"type\":\"TIMESTAMP_WITH_LOCAL_TIME_ZONE\",\"nullable\":false,\"precision\":6}",
            "{\"type\":\"TIMESTAMP_WITH_LOCAL_TIME_ZONE\",\"precision\":3}",
            "{\"type\":\"TIMESTAMP_WITH_LOCAL_TIME_ZONE\",\"nullable\":false,\"precision\":3}",
            "{\"type\":\"ARRAY\",\"element_type\":{\"type\":\"INTEGER\",\"nullable\":false}}",
            "{\"type\":\"ARRAY\",\"nullable\":false,\"element_type\":{\"type\":\"INTEGER\",\"nullable\":false}}",
            "{\"type\":\"MAP\",\"key_type\":{\"type\":\"BIGINT\",\"nullable\":false},\"value_type\":{\"type\":\"INTEGER\",\"nullable\":false}}",
            "{\"type\":\"MAP\",\"nullable\":false,\"key_type\":{\"type\":\"BIGINT\",\"nullable\":false},\"value_type\":{\"type\":\"INTEGER\",\"nullable\":false}}",
            "{\"type\":\"ROW\",\"fields\":[{\"name\":\"f0\",\"field_type\":{\"type\":\"BIGINT\"},\"field_id\":-1},{\"name\":\"f1\",\"field_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"field_id\":1},{\"name\":\"f2\",\"field_type\":{\"type\":\"STRING\"},\"field_id\":2}]}",
            "{\"type\":\"ROW\",\"nullable\":false,\"fields\":[{\"name\":\"f0\",\"field_type\":{\"type\":\"BIGINT\"},\"field_id\":-1},{\"name\":\"f1\",\"field_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"field_id\":1},{\"name\":\"f2\",\"field_type\":{\"type\":\"STRING\"},\"field_id\":2}]}"
        };
    }

    @Test
    void testJsonLackOfFieldId() {
        // some fields with field_id while others without field_id.
        String testJsonWithInconsistencyFieldId =
                "{\"type\":\"ROW\",\"nullable\":false,\"fields\":[{\"name\":\"f0\",\"field_type\":{\"type\":\"BIGINT\"}},{\"name\":\"f1\",\"field_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"field_id\":1},{\"name\":\"f2\",\"field_type\":{\"type\":\"STRING\"},\"field_id\":2}]}";
        DataType dataType =
                JsonSerdeUtils.readValue(
                        testJsonWithInconsistencyFieldId.getBytes(StandardCharsets.UTF_8),
                        DataTypeJsonSerde.INSTANCE);
        assertThat(dataType).isInstanceOf(RowType.class);
        assertEquals(
                dataType,
                DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.BIGINT()),
                                DataTypes.FIELD("f1", DataTypes.INT().copy(false), 1),
                                DataTypes.FIELD("f2", DataTypes.STRING(), 2))
                        .copy(false));
    }
}
