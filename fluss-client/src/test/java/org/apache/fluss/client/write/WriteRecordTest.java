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

package org.apache.fluss.client.write;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.record.MutationType;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link WriteRecord}. */
class WriteRecordTest {

    @Test
    void testKvUpsertRecordHasRow() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        byte[] key =
                new CompactedKeyEncoder(DATA1_ROW_TYPE, DATA1_SCHEMA_PK.getPrimaryKeyIndexes())
                        .encodeKey(row);
        WriteRecord writeRecord =
                WriteRecord.forUpsert(
                        DATA1_TABLE_INFO_PK,
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        row,
                        key,
                        key,
                        WriteFormat.COMPACTED_KV,
                        null);

        assertThat(writeRecord.getMutationType()).isEqualTo(MutationType.UPSERT);
        assertThat(writeRecord.getRow()).isEqualTo(row);
        assertThat(writeRecord.getKey()).isEqualTo(key);
    }

    @Test
    void testKvDeleteRecordHasNullRow() {
        CompactedRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        byte[] key =
                new CompactedKeyEncoder(DATA1_ROW_TYPE, DATA1_SCHEMA_PK.getPrimaryKeyIndexes())
                        .encodeKey(row);
        WriteRecord writeRecord =
                WriteRecord.forDelete(
                        DATA1_TABLE_INFO_PK,
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        key,
                        key,
                        WriteFormat.COMPACTED_KV,
                        null);

        assertThat(writeRecord.getMutationType()).isEqualTo(MutationType.DELETE);
        assertThat(writeRecord.getRow()).isNull();
        assertThat(writeRecord.getKey()).isEqualTo(key);
    }

    @Test
    void testAppendOnlyRecordHasNoKey() {
        CompactedRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        WriteRecord writeRecord =
                WriteRecord.forCompactedAppend(
                        DATA1_TABLE_INFO_PK, PhysicalTablePath.of(DATA1_TABLE_PATH_PK), row, null);

        assertThat(writeRecord.getRow()).isEqualTo(row);
        assertThat(writeRecord.getKey()).isNull();
    }

    @Test
    void testRetractMutationType() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        byte[] key =
                new CompactedKeyEncoder(DATA1_ROW_TYPE, DATA1_SCHEMA_PK.getPrimaryKeyIndexes())
                        .encodeKey(row);
        WriteRecord writeRecord =
                WriteRecord.forRetract(
                        DATA1_TABLE_INFO_PK,
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        row,
                        key,
                        key,
                        WriteFormat.COMPACTED_KV,
                        null,
                        org.apache.fluss.rpc.protocol.MergeMode.DEFAULT);

        assertThat(writeRecord.getMutationType()).isEqualTo(MutationType.RETRACT);
        assertThat(writeRecord.getRow()).isEqualTo(row);
        assertThat(writeRecord.getKey()).isEqualTo(key);
    }

    @ParameterizedTest(name = "reject null {3}")
    @MethodSource("nullRetractArguments")
    void testForRetractRejectsNullArgument(
            BinaryRow row, byte[] key, byte[] bucketKey, String parameterName) {
        assertThatThrownBy(
                        () ->
                                WriteRecord.forRetract(
                                        DATA1_TABLE_INFO_PK,
                                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                                        row,
                                        key,
                                        bucketKey,
                                        WriteFormat.COMPACTED_KV,
                                        null,
                                        org.apache.fluss.rpc.protocol.MergeMode.DEFAULT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining(parameterName + " must not be null");
    }

    private static Stream<Arguments> nullRetractArguments() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        byte[] key =
                new CompactedKeyEncoder(DATA1_ROW_TYPE, DATA1_SCHEMA_PK.getPrimaryKeyIndexes())
                        .encodeKey(row);
        return Stream.of(
                Arguments.of(null, key, key, "row"),
                Arguments.of(row, null, key, "key"),
                Arguments.of(row, key, null, "bucketKey"));
    }

    @Test
    void testForRetractNonKvFormatThrows() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        byte[] key =
                new CompactedKeyEncoder(DATA1_ROW_TYPE, DATA1_SCHEMA_PK.getPrimaryKeyIndexes())
                        .encodeKey(row);
        assertThatThrownBy(
                        () ->
                                WriteRecord.forRetract(
                                        DATA1_TABLE_INFO_PK,
                                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                                        row,
                                        key,
                                        key,
                                        WriteFormat.INDEXED_LOG,
                                        null,
                                        org.apache.fluss.rpc.protocol.MergeMode.DEFAULT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("writeFormat must be a KV format");
    }
}
