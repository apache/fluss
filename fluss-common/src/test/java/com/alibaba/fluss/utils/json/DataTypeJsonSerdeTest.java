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

import com.alibaba.fluss.types.ArrayType;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.DoubleType;
import com.alibaba.fluss.types.FloatType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.MapType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.SmallIntType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;
import com.alibaba.fluss.types.TinyIntType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Test for {@link DataTypeJsonSerde}. */
public class DataTypeJsonSerdeTest extends JsonSerdeTestBase<DataType> {

    DataTypeJsonSerdeTest() {
        super(DataTypeJsonSerde.INSTANCE);
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
                        new MapType(new BigIntType(), new IntType(false)),
                        RowType.of(new BigIntType(), new IntType(false), new StringType()));

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
            "{\"type\":\"MAP\",\"key_type\":{\"type\":\"BIGINT\"},\"value_type\":{\"type\":\"INTEGER\",\"nullable\":false}}",
            "{\"type\":\"MAP\",\"nullable\":false,\"key_type\":{\"type\":\"BIGINT\"},\"value_type\":{\"type\":\"INTEGER\",\"nullable\":false}}",
            "{\"type\":\"ROW\",\"fields\":[{\"name\":\"f0\",\"field_type\":{\"type\":\"BIGINT\"}},{\"name\":\"f1\",\"field_type\":{\"type\":\"INTEGER\",\"nullable\":false}},{\"name\":\"f2\",\"field_type\":{\"type\":\"STRING\"}}]}",
            "{\"type\":\"ROW\",\"nullable\":false,\"fields\":[{\"name\":\"f0\",\"field_type\":{\"type\":\"BIGINT\"}},{\"name\":\"f1\",\"field_type\":{\"type\":\"INTEGER\",\"nullable\":false}},{\"name\":\"f2\",\"field_type\":{\"type\":\"STRING\"}}]}",
        };
    }
}
