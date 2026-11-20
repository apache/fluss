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

package org.apache.fluss.client.table;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BucketCalculator}. */
class BucketCalculatorTest {

    private static final TablePath TABLE_PATH = TablePath.of("db", "bucket_test");

    @Test
    void testLogBucketKeyEncoding() {
        Schema schema =
                Schema.newBuilder()
                        .column("payload", DataTypes.STRING())
                        .column("other", DataTypes.BIGINT())
                        .column("id", DataTypes.INT())
                        .build();

        for (DataLakeFormat format :
                new DataLakeFormat[] {
                    null, DataLakeFormat.PAIMON, DataLakeFormat.ICEBERG, DataLakeFormat.LANCE
                }) {
            List<String> keys = Collections.singletonList("id");
            verifyRouting(
                    tableInfo(schema, keys, null, format, null),
                    GenericRow.of(BinaryString.fromString("value"), 73L, 42),
                    GenericRow.of(42));
        }
    }

    @Test
    void testLogBucketKeyOrderDiffersFromSchemaOrder() {
        Schema schema =
                Schema.newBuilder()
                        .column("payload", DataTypes.STRING())
                        .column("other", DataTypes.BIGINT())
                        .column("id", DataTypes.INT())
                        .build();
        List<String> keys = Arrays.asList("id", "other");

        verifyRouting(
                tableInfo(schema, keys, null, null, null),
                GenericRow.of(BinaryString.fromString("value"), 73L, 42),
                GenericRow.of(42, 73L));
    }

    @Test
    void testExplicitPrimaryKeyBucketKey() {
        Schema schema =
                Schema.newBuilder()
                        .column("payload", DataTypes.STRING())
                        .column("other", DataTypes.BIGINT())
                        .column("id", DataTypes.INT())
                        .primaryKey("id", "other")
                        .build();
        List<String> keys = Collections.singletonList("id");

        for (int version : new int[] {1, 2}) {
            for (DataLakeFormat format :
                    new DataLakeFormat[] {
                        null, DataLakeFormat.PAIMON, DataLakeFormat.HUDI, DataLakeFormat.LANCE
                    }) {
                verifyRouting(
                        tableInfo(schema, keys, null, format, version),
                        GenericRow.of(BinaryString.fromString("value"), 73L, 42),
                        GenericRow.of(42));
            }
        }
        // Iceberg cannot encode a composite primary key with KV V1, but KV V2 uses compacted PK
        // encoding.
        verifyRouting(
                tableInfo(schema, keys, null, DataLakeFormat.ICEBERG, 2),
                GenericRow.of(BinaryString.fromString("value"), 73L, 42),
                GenericRow.of(42));
    }

    @Test
    void testDefaultPrimaryKeyBucketKey() {
        Schema schema =
                Schema.newBuilder()
                        .column("payload", DataTypes.STRING())
                        .column("other", DataTypes.BIGINT())
                        .column("id", DataTypes.INT())
                        .primaryKey("id", "other")
                        .build();
        List<String> keys = Arrays.asList("id", "other");

        for (int version : new int[] {1, 2}) {
            for (DataLakeFormat format :
                    new DataLakeFormat[] {
                        null, DataLakeFormat.PAIMON, DataLakeFormat.HUDI, DataLakeFormat.LANCE
                    }) {
                verifyRouting(
                        tableInfo(schema, keys, null, format, version),
                        GenericRow.of(BinaryString.fromString("value"), 73L, 42),
                        GenericRow.of(42, 73L));
            }
        }
    }

    @Test
    void testIcebergSingleColumnDefaultPrimaryKey() {
        Schema schema =
                Schema.newBuilder()
                        .column("payload", DataTypes.STRING())
                        .column("other", DataTypes.BIGINT())
                        .column("id", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        for (int version : new int[] {1, 2}) {
            verifyRouting(
                    tableInfo(
                            schema,
                            Collections.singletonList("id"),
                            null,
                            DataLakeFormat.ICEBERG,
                            version),
                    GenericRow.of(BinaryString.fromString("value"), 73L, 42),
                    GenericRow.of(42));
        }
    }

    @Test
    void testLegacyTableWithoutKvVersion() {
        Schema schema =
                Schema.newBuilder()
                        .column("payload", DataTypes.STRING())
                        .column("other", DataTypes.BIGINT())
                        .column("id", DataTypes.INT())
                        .primaryKey("id", "other")
                        .build();

        List<String> keys = Arrays.asList("id", "other");
        TableInfo legacy = tableInfo(schema, keys, null, DataLakeFormat.PAIMON, null);
        TableInfo versionOne = tableInfo(schema, keys, null, DataLakeFormat.PAIMON, 1);
        InternalRow fullRow = GenericRow.of(BinaryString.fromString("value"), 73L, 42);
        byte[] legacyBytes =
                BucketCalculator.forBucketKeyRow(legacy).encodeKey(GenericRow.of(42, 73L));
        byte[] v1Bytes =
                BucketCalculator.forBucketKeyRow(versionOne).encodeKey(GenericRow.of(42, 73L));

        assertThat(legacyBytes).containsExactly(v1Bytes);
        verifyRouting(legacy, fullRow, GenericRow.of(42, 73L));
    }

    @Test
    void testPartitionedPhysicalPrimaryKey() {
        Schema schema =
                Schema.newBuilder()
                        .column("region", DataTypes.STRING())
                        .column("payload", DataTypes.STRING())
                        .column("other", DataTypes.BIGINT())
                        .column("id", DataTypes.INT())
                        .primaryKey("region", "id", "other")
                        .build();
        List<String> bucketKeys = Arrays.asList("id", "other");
        TableInfo tableInfo =
                tableInfo(schema, bucketKeys, Collections.singletonList("region"), null, 2);

        assertThat(tableInfo.getPrimaryKeys()).containsExactly("region", "id", "other");
        assertThat(tableInfo.getPhysicalPrimaryKeys()).containsExactly("id", "other");
        assertThat(tableInfo.isDefaultBucketKey()).isTrue();
        verifyRouting(
                tableInfo,
                GenericRow.of(
                        BinaryString.fromString("east"), BinaryString.fromString("value"), 73L, 42),
                GenericRow.of(42, 73L));
    }

    @Test
    void testIcebergCompositePrimaryKeyKvV1IsRejected() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("other", DataTypes.BIGINT())
                        .primaryKey("id", "other")
                        .build();
        TableInfo tableInfo =
                tableInfo(schema, Collections.singletonList("id"), null, DataLakeFormat.ICEBERG, 1);
        // The writer must preserve the existing rejection of Iceberg composite primary-key encoding
        // in KV V1.
        assertThatThrownBy(
                        () ->
                                KeyEncoder.ofPrimaryKeyEncoder(
                                        tableInfo.getRowType(),
                                        tableInfo.getPhysicalPrimaryKeys(),
                                        tableInfo.getTableConfig(),
                                        tableInfo.isDefaultBucketKey()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one field");
    }

    @Test
    void testPrimaryKeyByteReuse() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        for (DataLakeFormat format :
                new DataLakeFormat[] {
                    null, DataLakeFormat.PAIMON, DataLakeFormat.HUDI, DataLakeFormat.LANCE
                }) {
            TableInfo info = tableInfo(schema, Collections.singletonList("id"), null, format, 2);
            KeyEncoder primaryEncoder = primaryKeyEncoder(info);
            BucketCalculator calculator = BucketCalculator.forPrimaryKeyRow(info, primaryEncoder);
            InternalRow row = GenericRow.of(42, BinaryString.fromString("value"));
            byte[] primaryKey = primaryEncoder.encodeKey(row);
            byte[] bucketKey = calculator.encodeKey(row, primaryKey);
            if (format == DataLakeFormat.HUDI) {
                assertThat(bucketKey).isNotSameAs(primaryKey);
                assertThat(bucketKey).hasSize(4);
            } else {
                assertThat(bucketKey).isSameAs(primaryKey);
            }
        }
    }

    @Test
    void testInvalidBucketCount() {
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        TableInfo info = tableInfo(schema, Collections.singletonList("id"), null, null, null);
        BucketCalculator calculator = BucketCalculator.forBucketKeyRow(info);
        byte[] key = calculator.encodeKey(GenericRow.of(42));

        assertThatThrownBy(() -> calculator.getBucketId(key, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.getBucketId(key, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void verifyRouting(
            TableInfo tableInfo, InternalRow fullRow, InternalRow bucketKeyRow) {
        KeyEncoder primaryEncoder = tableInfo.hasPrimaryKey() ? primaryKeyEncoder(tableInfo) : null;
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
        // Reference the original writer encoder selection.
        KeyEncoder originalBucketEncoder =
                tableInfo.hasPrimaryKey()
                        ? KeyEncoder.ofBucketKeyEncoder(
                                tableInfo.getRowType(),
                                tableInfo.getBucketKeys(),
                                tableInfo.getTableConfig(),
                                tableInfo.isDefaultBucketKey(),
                                primaryEncoder)
                        : KeyEncoder.ofBucketKeyEncoder(
                                tableInfo.getRowType(), tableInfo.getBucketKeys(), lakeFormat);

        byte[] expectedKey = originalBucketEncoder.encodeKey(fullRow).clone();
        BucketCalculator writerCalculator =
                tableInfo.hasPrimaryKey()
                        ? BucketCalculator.forPrimaryKeyRow(tableInfo, primaryEncoder)
                        : BucketCalculator.forAppendRow(tableInfo);
        byte[] writerKey;
        if (tableInfo.hasPrimaryKey()) {
            byte[] primaryKey = primaryEncoder.encodeKey(fullRow);
            writerKey = writerCalculator.encodeKey(fullRow, primaryKey);
        } else {
            writerKey = writerCalculator.encodeKey(fullRow);
        }
        BucketCalculator readerCalculator = BucketCalculator.forBucketKeyRow(tableInfo);
        byte[] readerKey = readerCalculator.encodeKey(bucketKeyRow);
        assertThat(writerKey).containsExactly(expectedKey);
        assertThat(readerKey).containsExactly(expectedKey);
        BucketingFunction reference = BucketingFunction.of(lakeFormat);
        // Include non-power-of-two bucket counts and different physical partition layouts.
        for (int bucketCount : new int[] {1, 2, 3, 4, 7, 8, 16}) {
            int expectedBucket = reference.bucketing(expectedKey, bucketCount);
            assertThat(writerCalculator.getBucketId(writerKey, bucketCount))
                    .isEqualTo(expectedBucket);
            assertThat(readerCalculator.getBucketId(readerKey, bucketCount))
                    .isEqualTo(expectedBucket);
        }
    }

    private static KeyEncoder primaryKeyEncoder(TableInfo info) {
        return KeyEncoder.ofPrimaryKeyEncoder(
                info.getRowType(),
                info.getPhysicalPrimaryKeys(),
                info.getTableConfig(),
                info.isDefaultBucketKey());
    }

    private static TableInfo tableInfo(
            Schema schema,
            List<String> bucketKeys,
            @Nullable List<String> partitionKeys,
            @Nullable DataLakeFormat lakeFormat,
            @Nullable Integer kvVersion) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(7, bucketKeys.toArray(new String[0]));
        if (partitionKeys != null) {
            builder.partitionedBy(partitionKeys.toArray(new String[0]));
        }
        if (lakeFormat != null) {
            builder.property(ConfigOptions.TABLE_DATALAKE_FORMAT, lakeFormat);
        }
        if (kvVersion != null) {
            builder.property(ConfigOptions.TABLE_KV_FORMAT_VERSION, kvVersion);
        }
        return TableInfo.of(TABLE_PATH, 42L, 0, builder.build(), null, 0L, 0L);
    }
}
