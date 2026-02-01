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

package org.apache.fluss.lake.iceberg;

import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.LocalZonedTimestampNanoType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimestampNanoType;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Iceberg V3 type conversion in {@link FlussDataTypeToIcebergDataType}. */
class FlussDataTypeToIcebergV3TypesTest {

    @Test
    void testTimestampNanoType() {
        TimestampNanoType tsNano = new TimestampNanoType();
        Type icebergType = tsNano.accept(FlussDataTypeToIcebergDataType.INSTANCE);

        assertThat(icebergType).isInstanceOf(Types.TimestampNanoType.class);
        assertThat(((Types.TimestampNanoType) icebergType).shouldAdjustToUTC()).isFalse();
    }

    @Test
    void testTimestampNanoTypeNonNullable() {
        TimestampNanoType tsNano = new TimestampNanoType(false);
        Type icebergType = tsNano.accept(FlussDataTypeToIcebergDataType.INSTANCE);

        assertThat(icebergType).isInstanceOf(Types.TimestampNanoType.class);
    }

    @Test
    void testLocalZonedTimestampNanoType() {
        LocalZonedTimestampNanoType tsNanoLtz = new LocalZonedTimestampNanoType();
        Type icebergType = tsNanoLtz.accept(FlussDataTypeToIcebergDataType.INSTANCE);

        assertThat(icebergType).isInstanceOf(Types.TimestampNanoType.class);
        assertThat(((Types.TimestampNanoType) icebergType).shouldAdjustToUTC()).isTrue();
    }

    @Test
    void testLocalZonedTimestampNanoTypeNonNullable() {
        LocalZonedTimestampNanoType tsNanoLtz = new LocalZonedTimestampNanoType(false);
        Type icebergType = tsNanoLtz.accept(FlussDataTypeToIcebergDataType.INSTANCE);

        assertThat(icebergType).isInstanceOf(Types.TimestampNanoType.class);
    }

    @Test
    void testRowWithNanoTimestampTypes() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("ts_nano", DataTypes.TIMESTAMP_NS()),
                        DataTypes.FIELD("ts_nano_ltz", DataTypes.TIMESTAMP_NS_LTZ()));

        FlussDataTypeToIcebergDataType converter = new FlussDataTypeToIcebergDataType(rowType);
        Type icebergType = rowType.accept(converter);

        assertThat(icebergType).isInstanceOf(Types.StructType.class);
        Types.StructType structType = (Types.StructType) icebergType;

        assertThat(structType.fields()).hasSize(3);
        assertThat(structType.field("id").type()).isInstanceOf(Types.IntegerType.class);
        assertThat(structType.field("ts_nano").type()).isInstanceOf(Types.TimestampNanoType.class);
        assertThat(structType.field("ts_nano_ltz").type())
                .isInstanceOf(Types.TimestampNanoType.class);

        // Verify timezone handling
        Types.TimestampNanoType tsNano =
                (Types.TimestampNanoType) structType.field("ts_nano").type();
        Types.TimestampNanoType tsNanoLtz =
                (Types.TimestampNanoType) structType.field("ts_nano_ltz").type();

        assertThat(tsNano.shouldAdjustToUTC()).isFalse();
        assertThat(tsNanoLtz.shouldAdjustToUTC()).isTrue();
    }
}
