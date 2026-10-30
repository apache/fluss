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
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import io.github.fluss_contrib.rocksdb.ColumnFamilyOptions;
import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.TickerType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    @ParameterizedTest(name = "{0}, partitioned={2}")
    @MethodSource("prefixTypes")
    void testEachPrefixTypeParticipatesInFiltering(
            DataType type, List<Object> values, boolean partitioned, @TempDir Path dir)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("marker", DataTypes.STRING())
                        .column("candidate", type)
                        .column("id", DataTypes.INT())
                        .primaryKey("marker", "candidate", "id")
                        .build();
        TableDescriptor table =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "marker", "candidate")
                        .build();
        CompactedKeyEncoder encoder = new CompactedKeyEncoder(schema.getRowType());
        CompactedKeyEncoder prefixEncoder =
                new CompactedKeyEncoder(schema.getRowType(), new int[] {0, 1});
        for (boolean enabled : new boolean[] {false, true}) {
            Path dbDir = dir.resolve("bloom-" + enabled);
            Configuration config =
                    new Configuration()
                            .set(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS, partitioned);
            RocksDBResourceContainer resources =
                    new RocksDBResourceContainer(
                            config,
                            dbDir.toFile(),
                            true,
                            KvManager.getDefaultRateLimiter(),
                            null,
                            null,
                            enabled ? resolve(table) : null);
            ColumnFamilyOptions options =
                    resources.getColumnOptions().setDisableAutoCompactions(true);
            try (RocksDBKv kv = new RocksDBKvBuilder(dbDir.toFile(), resources, options).build();
                    FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                for (int file = 0; file < 2; file++) {
                    // Both SSTs span all query keys, so file-range pruning cannot replace Bloom
                    // filtering.
                    kv.put(encoder.encodeKey(prefixRow("0000", values.get(0), 0)), new byte[] {-1});
                    kv.put(encoder.encodeKey(prefixRow("zzzz", values.get(0), 0)), new byte[] {-1});
                    for (int marker = 0; marker < 8; marker++) {
                        // Each marker exists in both SSTs; only the candidate distinguishes their
                        // prefixes.
                        for (int value = file; value < values.size(); value += 2) {
                            putPrefixRows(
                                    kv, encoder, "m00" + marker, values.get(value), marker, value);
                        }
                        putPrefixRows(kv, encoder, "t00" + marker, values.get(0), marker, 0);
                    }
                    kv.db.flush(flush);
                }
                for (int value = 0; value < values.size(); value++) {
                    long before = seekFiltered(resources);
                    for (int marker = 0; marker < 8; marker++) {
                        assertPrefixRows(
                                kv,
                                prefixEncoder,
                                "m00" + marker,
                                values.get(value),
                                marker,
                                value);
                    }
                    if (enabled) {
                        // Use several prefixes so an individual Bloom false positive does not fail
                        // the test.
                        assertThat(seekFiltered(resources))
                                .as("SST filtering for %s value %s", type, value)
                                .isGreaterThan(before);
                    } else {
                        assertThat(seekFiltered(resources)).isEqualTo(before);
                    }
                }
                for (int marker = 0; marker < 8; marker++) {
                    assertPrefixRows(kv, prefixEncoder, "t00" + marker, values.get(0), marker, 0);
                }
                long before = seekFiltered(resources);
                for (int marker = 0; marker < 8; marker++) {
                    byte[] missing =
                            prefixEncoder.encodeKey(
                                    prefixRow("t00" + marker, values.get(values.size() - 1), 0));
                    try (RocksDBKv.PrefixLookup lookup = kv.openPrefixLookup(missing)) {
                        assertThat(lookup.readAll()).as("missing candidate for %s", type).isEmpty();
                    }
                }
                if (enabled) {
                    assertThat(seekFiltered(resources))
                            .as("missing-prefix filtering for %s", type)
                            .isGreaterThan(before);
                } else {
                    assertThat(seekFiltered(resources)).isEqualTo(before);
                }
            }
        }
    }

    private static Stream<Arguments> prefixTypes() {
        List<Arguments> cases = new ArrayList<>();
        addPrefixType(cases, DataTypes.BOOLEAN(), false, true);
        addPrefixType(
                cases,
                DataTypes.TINYINT(),
                (byte) 0,
                (byte) 1,
                Byte.MIN_VALUE,
                Byte.MAX_VALUE,
                (byte) -1);
        addPrefixType(
                cases,
                DataTypes.SMALLINT(),
                (short) 0,
                (short) 256,
                (short) 1,
                (short) 257,
                Short.MIN_VALUE,
                Short.MAX_VALUE,
                (short) -1);
        addPrefixType(
                cases,
                DataTypes.INT(),
                0,
                1,
                127,
                128,
                256,
                16383,
                16384,
                (1 << 21) - 1,
                1 << 21,
                (1 << 28) - 1,
                1 << 28,
                Integer.MAX_VALUE,
                -1,
                Integer.MIN_VALUE);
        List<Object> longs = new ArrayList<>(Arrays.asList(0L, 1L, 127L, 128L, 256L));
        for (int bits = 14; bits <= 56; bits += 7) {
            longs.add((1L << bits) - 1);
            longs.add(1L << bits);
        }
        longs.addAll(Arrays.asList(Long.MAX_VALUE, -1L, Long.MIN_VALUE));
        addPrefixType(cases, DataTypes.BIGINT(), longs.toArray());
        addPrefixType(
                cases, DataTypes.DATE(), -719528, -1, 0, 127, 128, 256, 16383, 16384, 2932896);
        addPrefixType(
                cases,
                DataTypes.TIME(3),
                0,
                1,
                127,
                128,
                256,
                16383,
                16384,
                2097151,
                2097152,
                86399999);
        addPrefixType(
                cases,
                DataTypes.FLOAT(),
                0.0f,
                2.0f,
                1.0f,
                -1.0f,
                -0.0f,
                Float.MIN_VALUE,
                Float.MAX_VALUE,
                Float.NEGATIVE_INFINITY,
                Float.POSITIVE_INFINITY,
                Float.NaN);
        addPrefixType(
                cases,
                DataTypes.DOUBLE(),
                0.0d,
                2.0d,
                1.0d,
                -1.0d,
                -0.0d,
                Double.MIN_VALUE,
                Double.MAX_VALUE,
                Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY,
                Double.NaN);
        addPrefixType(
                cases,
                DataTypes.CHAR(128),
                BinaryString.fromString(repeated('a', 127) + "b"),
                BinaryString.fromString(repeated('a', 127) + "c"),
                BinaryString.fromString(repeated('a', 127) + "中"));
        List<Object> strings = new ArrayList<>();
        strings.add(BinaryString.fromString(""));
        for (int length : new int[] {1, 127, 128, 16383, 16384}) {
            strings.add(BinaryString.fromString(repeated('a', length - 1) + "b"));
            strings.add(BinaryString.fromString(repeated('a', length - 1) + "c"));
        }
        strings.add(BinaryString.fromString("a\0b"));
        strings.add(BinaryString.fromString("中文"));
        strings.add(BinaryString.fromString("😀"));
        addPrefixType(cases, DataTypes.STRING(), strings.toArray());
        addPrefixType(cases, DataTypes.BINARY(128), binaryValue(128, 0), binaryValue(128, 255));
        List<Object> bytes = new ArrayList<>();
        bytes.add(new byte[0]);
        for (int length : new int[] {1, 127, 128, 16383, 16384}) {
            bytes.add(binaryValue(length, 0));
            bytes.add(binaryValue(length, 255));
        }
        addPrefixType(cases, DataTypes.BYTES(), bytes.toArray());
        for (int precision : new int[] {18, 19}) {
            List<Object> decimals = new ArrayList<>();
            for (long unscaled : new long[] {0, 256, 127, 128, 129, -1, -128, -129}) {
                decimals.add(decimal(BigInteger.valueOf(unscaled), precision));
            }
            BigInteger max = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
            decimals.add(decimal(max, precision));
            decimals.add(decimal(max.negate(), precision));
            addPrefixType(cases, DataTypes.DECIMAL(precision, 2), decimals.toArray());
        }
        for (int precision : new int[] {3, 4, 9}) {
            for (boolean ltz : new boolean[] {false, true}) {
                List<Object> timestamps = new ArrayList<>();
                for (long millis :
                        new long[] {-1, 0, 127, 128, 256, 16383, 16384, 253402300799999L}) {
                    timestamps.add(timestamp(millis, 0, ltz));
                }
                if (precision > 3) {
                    int[] nanos =
                            precision == 4
                                    ? new int[] {100000, 200000, 900000}
                                    : new int[] {127, 128, 256, 16383, 16384, 999999};
                    for (int nano : nanos) {
                        timestamps.add(timestamp(0, nano, ltz));
                    }
                }
                addPrefixType(
                        cases,
                        ltz ? DataTypes.TIMESTAMP_LTZ(precision) : DataTypes.TIMESTAMP(precision),
                        timestamps.toArray());
            }
        }
        return cases.stream();
    }

    private static String repeated(char value, int length) {
        char[] chars = new char[length];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static byte[] binaryValue(int length, int lastByte) {
        byte[] bytes = new byte[length];
        bytes[length - 1] = (byte) lastByte;
        return bytes;
    }

    private static Decimal decimal(BigInteger unscaled, int precision) {
        return Decimal.fromBigDecimal(new BigDecimal(unscaled, 2), precision, 2);
    }

    private static Object timestamp(long millis, int nanos, boolean ltz) {
        return ltz
                ? TimestampLtz.fromEpochMillis(millis, nanos)
                : TimestampNtz.fromMillis(millis, nanos);
    }

    private static void addPrefixType(List<Arguments> cases, DataType type, Object... values) {
        for (boolean partitioned : new boolean[] {false, true}) {
            cases.add(Arguments.of(type, Arrays.asList(values), partitioned));
        }
    }

    private static GenericRow prefixRow(String marker, Object value, int id) {
        return GenericRow.of(BinaryString.fromString(marker), value, id);
    }

    private static void putPrefixRows(
            RocksDBKv kv,
            CompactedKeyEncoder encoder,
            String marker,
            Object value,
            int markerIndex,
            int valueIndex)
            throws Exception {
        int[] ids = {-1, 0, 128};
        for (int row = 0; row < ids.length; row++) {
            kv.put(
                    encoder.encodeKey(prefixRow(marker, value, ids[row])),
                    new byte[] {(byte) markerIndex, (byte) valueIndex, (byte) row});
        }
    }

    private static void assertPrefixRows(
            RocksDBKv kv,
            CompactedKeyEncoder encoder,
            String marker,
            Object value,
            int markerIndex,
            int valueIndex)
            throws Exception {
        try (RocksDBKv.PrefixLookup lookup =
                kv.openPrefixLookup(encoder.encodeKey(prefixRow(marker, value, 0)))) {
            assertThat(lookup.readAll())
                    .as("prefix %s, value %s", marker, valueIndex)
                    .containsExactlyInAnyOrder(
                            new byte[] {(byte) markerIndex, (byte) valueIndex, 0},
                            new byte[] {(byte) markerIndex, (byte) valueIndex, 1},
                            new byte[] {(byte) markerIndex, (byte) valueIndex, 2});
        }
    }

    private static long seekFiltered(RocksDBResourceContainer resources) {
        return resources.getStatistics().getTickerCount(TickerType.NON_LAST_LEVEL_SEEK_FILTERED);
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
