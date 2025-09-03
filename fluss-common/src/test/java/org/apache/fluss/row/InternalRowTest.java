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

package org.apache.fluss.row;

import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test of {@link org.apache.fluss.row.InternalRow}. */
public class InternalRowTest {

    @Test
    void testGetDataClass() {
        assertThat(InternalRow.getDataClass(DataTypes.CHAR(10))).isEqualTo(BinaryString.class);
        assertThat(InternalRow.getDataClass(DataTypes.STRING())).isEqualTo(BinaryString.class);
        assertThat(InternalRow.getDataClass(DataTypes.BOOLEAN())).isEqualTo(Boolean.class);
        assertThat(InternalRow.getDataClass(DataTypes.BINARY(10))).isEqualTo(byte[].class);
        assertThat(InternalRow.getDataClass(DataTypes.BYTES())).isEqualTo(byte[].class);
        assertThat(InternalRow.getDataClass(DataTypes.DECIMAL(5, 2))).isEqualTo(Decimal.class);
        assertThat(InternalRow.getDataClass(DataTypes.TINYINT())).isEqualTo(Byte.class);
        assertThat(InternalRow.getDataClass(DataTypes.SMALLINT())).isEqualTo(Short.class);
        assertThat(InternalRow.getDataClass(DataTypes.INT())).isEqualTo(Integer.class);
        assertThat(InternalRow.getDataClass(DataTypes.DATE())).isEqualTo(Integer.class);
        assertThat(InternalRow.getDataClass(DataTypes.TIME())).isEqualTo(Integer.class);
        assertThat(InternalRow.getDataClass(DataTypes.BIGINT())).isEqualTo(Long.class);
        assertThat(InternalRow.getDataClass(DataTypes.FLOAT())).isEqualTo(Float.class);
        assertThat(InternalRow.getDataClass(DataTypes.DOUBLE())).isEqualTo(Double.class);
        assertThat(InternalRow.getDataClass(DataTypes.TIMESTAMP())).isEqualTo(TimestampNtz.class);
        assertThat(InternalRow.getDataClass(DataTypes.TIMESTAMP_LTZ()))
                .isEqualTo(TimestampLtz.class);
        assertThatThrownBy(() -> InternalRow.getDataClass(DataTypes.ARRAY(DataTypes.TIMESTAMP())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                InternalRow.getDataClass(
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.TIMESTAMP())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                InternalRow.getDataClass(
                                        DataTypes.ROW(new DataField("a", DataTypes.INT()))))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
