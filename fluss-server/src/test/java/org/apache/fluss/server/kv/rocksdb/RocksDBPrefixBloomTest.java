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

package org.apache.fluss.server.kv.rocksdb;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KeyPrefixLayout;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.kv.TableKeyPrefixLayoutResolver;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import io.github.fluss_contrib.rocksdb.ColumnFamilyOptions;
import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.TickerType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests compatible layouts, unsupported-table fallback and native prefix Bloom filtering. */
class RocksDBPrefixBloomTest {

    @Test
    void testNonPrimaryKeyTableIsSkipped() {
        TableDescriptor table =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("value", DataTypes.STRING()).build())
                        .build();
        assertThat(resolve(table)).isNull();
    }

    @Test
    void testNonLeadingBucketKeysAreSkipped() {
        TableDescriptor table = table().distributedBy(1, "id").build();
        assertThat(resolve(table)).isNull();
    }

    @ParameterizedTest
    @EnumSource(
            value = DataLakeFormat.class,
            names = {"PAIMON", "ICEBERG", "HUDI", "LANCE"})
    void testVersionTwoUsesCompactedPrimaryKeyForCustomBucketKeys(DataLakeFormat lakeFormat) {
        TableDescriptor table =
                table().property(ConfigOptions.TABLE_KV_FORMAT_VERSION.key(), "2")
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT.key(), lakeFormat.name())
                        .build();
        assertThat(resolve(table)).isNotNull();
    }

    @Test
    void testUnsupportedPaimonEncodingFallsBack() {
        TableDescriptor table =
                table().property(ConfigOptions.TABLE_KV_FORMAT_VERSION.key(), "1")
                        .property(
                                ConfigOptions.TABLE_DATALAKE_FORMAT.key(),
                                DataLakeFormat.PAIMON.name())
                        .build();
        assertThat(resolve(table)).isNull();
    }

    @Test
    void testUnsupportedEncoderConstructionFallsBack() {
        TableDescriptor table =
                table().property(ConfigOptions.TABLE_KV_FORMAT_VERSION.key(), "1")
                        .property(
                                ConfigOptions.TABLE_DATALAKE_FORMAT.key(),
                                DataLakeFormat.ICEBERG.name())
                        .build();
        assertThat(resolve(table)).isNull();
    }

    @Test
    void testLegacyHudiStillUsesCompactedPrimaryKeys() {
        TableDescriptor table =
                table().property(ConfigOptions.TABLE_KV_FORMAT_VERSION.key(), "1")
                        .property(
                                ConfigOptions.TABLE_DATALAKE_FORMAT.key(),
                                DataLakeFormat.HUDI.name())
                        .build();
        assertThat(resolve(table)).isNotNull();
    }

    @Test
    void testUnsupportedDefaultBucketKeyEncodingIsSkipped() {
        TableDescriptor table =
                table().distributedBy(1)
                        .property(ConfigOptions.TABLE_KV_FORMAT_VERSION.key(), "2")
                        .property(
                                ConfigOptions.TABLE_DATALAKE_FORMAT.key(),
                                DataLakeFormat.PAIMON.name())
                        .build();
        assertThat(resolve(table)).isNull();
    }

    @Test
    void testPartitionFieldsAreExcludedFromPhysicalPrefix() {
        TableDescriptor table =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("dt", DataTypes.STRING())
                                        .column("bucket", DataTypes.STRING())
                                        .column("id", DataTypes.INT())
                                        .primaryKey("dt", "bucket", "id")
                                        .build())
                        .partitionedBy("dt")
                        .distributedBy(1, "bucket")
                        .build();
        assertThat(resolve(table)).isNotNull();
        TableInfo info =
                TableInfo.of(TablePath.of("db", "table"), 1L, 1, table, "/tmp/fluss", 0L, 0L);
        assertThat(TableKeyPrefixLayoutResolver.resolve(info)).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNativeFilteringAndExistingSstCompatibility(boolean partitioned, @TempDir Path dir)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("bucket", DataTypes.STRING())
                        .column("flag", DataTypes.BOOLEAN())
                        .column("tiny", DataTypes.TINYINT())
                        .column("small", DataTypes.SMALLINT())
                        .column("integer", DataTypes.INT())
                        .column("long", DataTypes.BIGINT())
                        .column("date", DataTypes.DATE())
                        .column("time", DataTypes.TIME())
                        .column("float", DataTypes.FLOAT())
                        .column("double", DataTypes.DOUBLE())
                        .column("char", DataTypes.CHAR(8))
                        .column("binary", DataTypes.BINARY(8))
                        .column("bytes", DataTypes.BYTES())
                        .column("decimal", DataTypes.DECIMAL(18, 2))
                        .column("wide_decimal", DataTypes.DECIMAL(38, 2))
                        .column("timestamp", DataTypes.TIMESTAMP(3))
                        .column("wide_timestamp", DataTypes.TIMESTAMP(9))
                        .column("ltz", DataTypes.TIMESTAMP_LTZ(3))
                        .column("wide_ltz", DataTypes.TIMESTAMP_LTZ(9))
                        .column("id", DataTypes.INT())
                        .primaryKey(
                                "bucket",
                                "flag",
                                "tiny",
                                "small",
                                "integer",
                                "long",
                                "date",
                                "time",
                                "float",
                                "double",
                                "char",
                                "binary",
                                "bytes",
                                "decimal",
                                "wide_decimal",
                                "timestamp",
                                "wide_timestamp",
                                "ltz",
                                "wide_ltz",
                                "id")
                        .build();
        List<String> bucketKeys = new ArrayList<>(schema.getRowType().getFieldNames());
        bucketKeys.remove("id");
        TableDescriptor table =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, bucketKeys.toArray(new String[0]))
                        .build();
        RowType rowType = schema.getRowType();
        CompactedKeyEncoder encoder = new CompactedKeyEncoder(rowType);
        CompactedKeyEncoder prefixEncoder =
                new CompactedKeyEncoder(rowType, IntStream.range(0, bucketKeys.size()).toArray());
        Configuration config = new Configuration();
        config.set(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS, partitioned);
        byte[] oldKey = encoder.encodeKey(compositeRow("old", 1));
        // An existing SST without a prefix extractor must remain readable after enabling it.
        RocksDBResourceContainer legacyResources =
                new RocksDBResourceContainer(config, dir.toFile());
        try (RocksDBKv kv =
                new RocksDBKvBuilder(
                                dir.toFile(), legacyResources, legacyResources.getColumnOptions())
                        .build()) {
            kv.put(oldKey, oldKey);
            try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                kv.db.flush(flush);
            }
        }
        for (int reopen = 0; reopen < 2; reopen++) {
            RocksDBResourceContainer resources =
                    new RocksDBResourceContainer(
                            config,
                            dir.toFile(),
                            true,
                            KvManager.getDefaultRateLimiter(),
                            null,
                            null,
                            resolve(table));
            ColumnFamilyOptions options =
                    resources.getColumnOptions().setDisableAutoCompactions(true);
            try (RocksDBKv kv = new RocksDBKvBuilder(dir.toFile(), resources, options).build()) {
                if (reopen == 0) {
                    for (String bucket : Arrays.asList("000", "mid", "zzz")) {
                        for (int id : new int[] {-1, 0, 128}) {
                            byte[] key = encoder.encodeKey(compositeRow(bucket, id));
                            kv.put(key, key);
                        }
                    }
                    try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                        kv.db.flush(flush);
                    }
                }
                assertThat(kv.get(oldKey)).isEqualTo(oldKey);
                try (RocksDBKv.PrefixLookup lookup =
                        kv.openPrefixLookup(prefixEncoder.encodeKey(compositeRow("old", 0)))) {
                    assertThat(lookup.readAll()).containsExactly(oldKey);
                }
                byte[][] expected = new byte[3][];
                int index = 0;
                for (int id : new int[] {-1, 0, 128}) {
                    expected[index++] = encoder.encodeKey(compositeRow("mid", id));
                }
                try (RocksDBKv.PrefixLookup lookup =
                        kv.openPrefixLookup(prefixEncoder.encodeKey(compositeRow("mid", 0)))) {
                    assertThat(lookup.readAll()).containsExactlyInAnyOrder(expected);
                }
                long before =
                        resources
                                .getStatistics()
                                .getTickerCount(TickerType.NON_LAST_LEVEL_SEEK_FILTERED);
                for (int probe = 100; probe < 120; probe++) {
                    try (RocksDBKv.PrefixLookup lookup =
                            kv.openPrefixLookup(
                                    prefixEncoder.encodeKey(
                                            compositeRow(Integer.toString(probe), 0)))) {
                        assertThat(lookup.readAll()).isEmpty();
                    }
                }
                assertThat(
                                resources
                                        .getStatistics()
                                        .getTickerCount(TickerType.NON_LAST_LEVEL_SEEK_FILTERED))
                        .isGreaterThan(before);
                kv.db.compactRange();
            }
        }
    }

    private static GenericRow compositeRow(String bucket, int id) {
        return GenericRow.of(
                BinaryString.fromString(bucket),
                true,
                (byte) -1,
                (short) -2,
                -1,
                Long.MIN_VALUE,
                -3,
                12345,
                -1.25f,
                -2.5d,
                BinaryString.fromString("abc"),
                new byte[] {0, (byte) 255},
                new byte[130],
                Decimal.fromUnscaledLong(-12345, 18, 2),
                Decimal.fromBigDecimal(new BigDecimal("-123456789012345678901.23"), 38, 2),
                TimestampNtz.fromMillis(-1234),
                TimestampNtz.fromMillis(-1234, 999999),
                TimestampLtz.fromEpochMillis(-1234),
                TimestampLtz.fromEpochMillis(-1234, 999999),
                id);
    }

    private static KeyPrefixLayout resolve(TableDescriptor table) {
        return TableKeyPrefixLayoutResolver.resolve(
                TableInfo.of(
                        TablePath.of("db", "table"),
                        1L,
                        1,
                        table.withBucketCount(1),
                        "/tmp/fluss",
                        0L,
                        0L));
    }

    private static TableDescriptor.Builder table() {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("bucket", DataTypes.STRING())
                                .column("id", DataTypes.INT())
                                .primaryKey("bucket", "id")
                                .build())
                .distributedBy(1, "bucket");
    }
}
