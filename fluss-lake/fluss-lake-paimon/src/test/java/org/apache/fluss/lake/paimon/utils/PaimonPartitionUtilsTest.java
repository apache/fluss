/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.PartitionUtils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PaimonPartitionUtils}. */
class PaimonPartitionUtilsTest {

    /** Each type's lake-side string must equal the Fluss-side name ({@code convertValueOfType}). */
    @Test
    void testTypesMatchFlussName() {
        int epochDay = (int) LocalDate.of(2024, 3, 1).toEpochDay();
        int milliOfDay = ((12 * 60 + 34) * 60 + 56) * 1000 + 789;
        long ms = 1709294096123L;
        int nanos = 456000; // multiple of 1000 to survive microsecond precision
        byte[] bytes = {1, 2, 3, (byte) 0xAB};

        assertMatches(
                DataTypes.BOOLEAN(), w -> w.writeBoolean(0, true), true, DataTypeRoot.BOOLEAN);
        assertMatches(
                DataTypes.TINYINT(), w -> w.writeByte(0, (byte) 7), (byte) 7, DataTypeRoot.TINYINT);
        assertMatches(
                DataTypes.SMALLINT(),
                w -> w.writeShort(0, (short) 300),
                (short) 300,
                DataTypeRoot.SMALLINT);
        assertMatches(DataTypes.INT(), w -> w.writeInt(0, 42), 42, DataTypeRoot.INTEGER);
        assertMatches(DataTypes.BIGINT(), w -> w.writeLong(0, 123L), 123L, DataTypeRoot.BIGINT);
        assertMatches(DataTypes.FLOAT(), w -> w.writeFloat(0, 1.5f), 1.5f, DataTypeRoot.FLOAT);
        assertMatches(DataTypes.DOUBLE(), w -> w.writeDouble(0, 2.5d), 2.5d, DataTypeRoot.DOUBLE);
        assertMatches(DataTypes.DATE(), w -> w.writeInt(0, epochDay), epochDay, DataTypeRoot.DATE);
        assertMatches(
                DataTypes.TIME(),
                w -> w.writeInt(0, milliOfDay),
                milliOfDay,
                DataTypeRoot.TIME_WITHOUT_TIME_ZONE);
        assertMatches(DataTypes.BYTES(), w -> w.writeBinary(0, bytes), bytes, DataTypeRoot.BYTES);
        assertMatches(
                DataTypes.TIMESTAMP(6),
                w -> w.writeTimestamp(0, Timestamp.fromEpochMillis(ms, nanos), 6),
                TimestampNtz.fromMillis(ms, nanos),
                DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        assertMatches(
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6),
                w -> w.writeTimestamp(0, Timestamp.fromEpochMillis(ms, nanos), 6),
                TimestampLtz.fromEpochMillis(ms, nanos),
                DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    @Test
    void testStringPartition() {
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeString(0, BinaryString.fromString("a"));
        writer.complete();

        assertThat(PaimonPartitionUtils.partitionValues(partition, RowType.of(DataTypes.STRING())))
                .containsExactly("a");
    }

    @Test
    void testMultiColumnPartition() {
        int epochDay = (int) LocalDate.of(2024, 3, 1).toEpochDay();
        BinaryRow partition = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeInt(0, epochDay);
        writer.writeInt(1, 42);
        writer.complete();

        assertThat(
                        PaimonPartitionUtils.partitionValues(
                                partition, RowType.of(DataTypes.DATE(), DataTypes.INT())))
                .containsExactly("2024-03-01", "42");
    }

    private static void assertMatches(
            DataType paimonType,
            Consumer<BinaryRowWriter> writeValue,
            Object flussValue,
            DataTypeRoot flussRoot) {
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writeValue.accept(writer);
        writer.complete();
        assertThat(PaimonPartitionUtils.partitionValues(partition, RowType.of(paimonType)))
                .as("type %s", flussRoot)
                .containsExactly(PartitionUtils.convertValueOfType(flussValue, flussRoot));
    }
}
