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

package org.apache.fluss.connector.trino.typeutils;

import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.StringType;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType as TrinoBooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FlussTypeUtils}.
 */
class FlussTypeUtilsTest {

    private final TypeManager typeManager = Mockito.mock(TypeManager.class);

    @Test
    void testBooleanTypeConversion() {
        DataType flussType = new BooleanType();
        Type trinoType = FlussTypeUtils.toTrinoType(flussType, typeManager);
        assertThat(trinoType).isEqualTo(TrinoBooleanType.BOOLEAN);
    }

    @Test
    void testIntegerTypeConversion() {
        DataType flussType = new IntType();
        Type trinoType = FlussTypeUtils.toTrinoType(flussType, typeManager);
        assertThat(trinoType).isEqualTo(IntegerType.INTEGER);
    }

    @Test
    void testBigIntTypeConversion() {
        DataType flussType = new BigIntType();
        Type trinoType = FlussTypeUtils.toTrinoType(flussType, typeManager);
        assertThat(trinoType).isEqualTo(BigintType.BIGINT);
    }

    @Test
    void testStringTypeConversion() {
        DataType flussType = new StringType();
        Type trinoType = FlussTypeUtils.toTrinoType(flussType, typeManager);
        assertThat(trinoType).isEqualTo(VarcharType.VARCHAR);
    }

    @Test
    void testNullableType() {
        DataType flussType = new IntType(true);
        Type trinoType = FlussTypeUtils.toTrinoType(flussType, typeManager);
        assertThat(trinoType).isEqualTo(IntegerType.INTEGER);
    }
}
