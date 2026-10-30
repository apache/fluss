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

package org.apache.fluss.trino;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableMap;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_FORMAT;
import static org.apache.fluss.config.ConfigOptions.TABLE_KV_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussBucketPruner}. */
class FlussBucketPrunerTest {

    private static final long TABLE_ID = 42L;

    @ParameterizedTest
    @ValueSource(strings = {"a,b", "__null__", "__empty__"})
    void testRejectedHudiPredicateFallsBackToFullScan(String value) {
        Schema schema =
                Schema.newBuilder()
                        .column("k1", DataTypes.STRING())
                        .column("k2", DataTypes.STRING())
                        .primaryKey("k1", "k2")
                        .build();
        Configuration config = new Configuration();
        config.set(TABLE_DATALAKE_FORMAT, DataLakeFormat.HUDI);
        TableInfo table =
                new TableInfo(
                        TablePath.of("db", "t"),
                        TABLE_ID,
                        0,
                        schema,
                        Arrays.asList("k1", "k2"),
                        Collections.emptyList(),
                        8,
                        config,
                        new Configuration(),
                        null,
                        null,
                        0L,
                        0L);
        FlussTableScanValidator.validateTable(
                new FlussTableHandle("db", "t", "db", "t", TABLE_ID, 0, TupleDomain.all()), table);
        Map<FlussColumnHandle, Domain> domains = new HashMap<>();
        domains.put(new FlussColumnHandle("k1", 0), Domain.singleValue(VARCHAR, utf8Slice(value)));
        domains.put(new FlussColumnHandle("k2", 1), Domain.singleValue(VARCHAR, utf8Slice("x")));
        assertThat(FlussBucketPruner.create(table, TupleDomain.withColumnDomains(domains)))
                .isEmpty();
    }

    @Test
    void testNonPartitionedLogRoutingMatchesWriterEncoder() {
        Schema schema =
                Schema.newBuilder()
                        .column("user_id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .build();

        TableInfo table = table(schema, Arrays.asList("user_id"), Collections.emptyList(), 8);
        TupleDomain<FlussColumnHandle> constraint =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                new FlussColumnHandle("user_id", 0),
                                Domain.singleValue(INTEGER, 123L)));
        FlussBucketPruner pruner =
                FlussBucketPruner.create(table, constraint)
                        .orElseThrow(() -> new AssertionError("Expected bucket pruning"));
        GenericRow writerRow = new GenericRow(2);
        writerRow.setField(0, 123);
        writerRow.setField(1, BinaryString.fromString("unused"));

        byte[] writerKey =
                KeyEncoder.ofBucketKeyEncoder(table.getRowType(), table.getBucketKeys(), null)
                        .encodeKey(writerRow);

        int expected = BucketingFunction.of(null).bucketing(writerKey, 8);

        assertThat(pruner.getBucketId(8)).isEqualTo(expected);
    }

    @Test
    void testNonPartitionedDefaultPrimaryKeyUsesPrimaryKeyEncoding() {
        Schema schema =
                Schema.newBuilder()
                        .column("user_id", DataTypes.BIGINT())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("user_id")
                        .build();

        TableInfo table = table(schema, Arrays.asList("user_id"), Collections.emptyList(), 8);
        assertThat(table.isDefaultBucketKey()).isTrue();
        FlussBucketPruner pruner =
                FlussBucketPruner.create(
                                table,
                                TupleDomain.withColumnDomains(
                                        ImmutableMap.of(
                                                new FlussColumnHandle("user_id", 0),
                                                Domain.singleValue(BIGINT, 123L))))
                        .orElseThrow(() -> new AssertionError("Expected bucket pruning"));
        GenericRow lookupRow = new GenericRow(1);
        lookupRow.setField(0, 123L);
        KeyEncoder primary =
                KeyEncoder.ofPrimaryKeyEncoder(
                        table.getRowType().project(table.getPrimaryKeys()),
                        table.getPhysicalPrimaryKeys(),
                        table.getTableConfig(),
                        true);

        byte[] key = primary.encodeKey(lookupRow);
        int expected = BucketingFunction.of(null).bucketing(key, 8);
        assertThat(pruner.getBucketId(8)).isEqualTo(expected);
    }

    @Test
    void testPartitionedDefaultPkCanRouteWithoutPartitionKeyConstraint() {
        Schema schema =
                Schema.newBuilder()
                        .column("dt", DataTypes.STRING())
                        .column("id", DataTypes.BIGINT())
                        .primaryKey("dt", "id")
                        .build();

        TableInfo table = table(schema, Arrays.asList("id"), Arrays.asList("dt"), 8);
        assertThat(table.isDefaultBucketKey()).isTrue();
        FlussBucketPruner pruner =
                FlussBucketPruner.create(
                                table,
                                TupleDomain.withColumnDomains(
                                        ImmutableMap.of(
                                                new FlussColumnHandle("id", 1),
                                                Domain.singleValue(BIGINT, 99L))))
                        .orElseThrow(() -> new AssertionError("Expected bucket pruning"));

        /*
         * Physical PK encoding uses id rather than dt.
         */
        GenericRow lookupRow = new GenericRow(2);
        lookupRow.setField(0, BinaryString.fromString("2026-10-20"));
        lookupRow.setField(1, 99L);

        byte[] key =
                KeyEncoder.ofPrimaryKeyEncoder(
                                table.getRowType().project(table.getPrimaryKeys()),
                                table.getPhysicalPrimaryKeys(),
                                table.getTableConfig(),
                                true)
                        .encodeKey(lookupRow);
        assertThat(pruner.getBucketId(4)).isEqualTo(BucketingFunction.of(null).bucketing(key, 4));
        assertThat(pruner.getBucketId(8)).isEqualTo(BucketingFunction.of(null).bucketing(key, 8));
    }

    @Test
    void testPartitionSpecificBucketCounts() {
        Schema schema =
                Schema.newBuilder()
                        .column("dt", DataTypes.STRING())
                        .column("user_id", DataTypes.INT())
                        .build();
        TableInfo table = table(schema, Arrays.asList("user_id"), Arrays.asList("dt"), 8);
        FlussBucketPruner pruner =
                FlussBucketPruner.create(
                                table,
                                TupleDomain.withColumnDomains(
                                        ImmutableMap.of(
                                                new FlussColumnHandle("user_id", 1),
                                                Domain.singleValue(INTEGER, 123L))))
                        .orElseThrow(() -> new AssertionError("Expected bucket pruning"));
        GenericRow writerRow = new GenericRow(2);
        writerRow.setField(0, BinaryString.fromString("2026-10-20"));
        writerRow.setField(1, 123);

        byte[] key =
                KeyEncoder.ofBucketKeyEncoder(table.getRowType(), table.getBucketKeys(), null)
                        .encodeKey(writerRow);
        assertThat(pruner.getBucketId(4)).isEqualTo(BucketingFunction.of(null).bucketing(key, 4));
        assertThat(pruner.getBucketId(8)).isEqualTo(BucketingFunction.of(null).bucketing(key, 8));
    }

    @Test
    void testUnsupportedDomainDoesNotCreatePruner() {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .build();
        TableInfo table = table(schema, Arrays.asList("a", "b"), Collections.emptyList(), 4);
        assertThat(FlussBucketPruner.create(table, TupleDomain.all())).isEmpty();
        assertThat(
                        FlussBucketPruner.create(
                                table,
                                TupleDomain.withColumnDomains(
                                        ImmutableMap.of(
                                                new FlussColumnHandle("a", 0),
                                                Domain.singleValue(INTEGER, 1L)))))
                .isEmpty();
        assertThat(
                        FlussBucketPruner.create(
                                table,
                                TupleDomain.withColumnDomains(
                                        ImmutableMap.of(
                                                new FlussColumnHandle("a", 0),
                                                Domain.singleValue(INTEGER, 1L),
                                                new FlussColumnHandle("b", 1),
                                                Domain.singleValue(BIGINT, 2L)))))
                .isEmpty();
        assertThat(
                        FlussBucketPruner.create(
                                table,
                                TupleDomain.withColumnDomains(
                                        ImmutableMap.of(
                                                new FlussColumnHandle("a", 0),
                                                Domain.multipleValues(
                                                        INTEGER, Arrays.asList(1L, 2L)),
                                                new FlussColumnHandle("b", 1),
                                                Domain.singleValue(INTEGER, 2L)))))
                .isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testInvalidBucketCountFails(int count) {
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        TableInfo table =
                table(schema, Collections.singletonList("id"), Collections.emptyList(), 4);
        FlussBucketPruner pruner =
                FlussBucketPruner.create(
                                table,
                                TupleDomain.withColumnDomains(
                                        ImmutableMap.of(
                                                new FlussColumnHandle("id", 0),
                                                Domain.singleValue(INTEGER, 5L))))
                        .orElseThrow(() -> new AssertionError("Expected bucket pruning"));
        assertThatThrownBy(() -> pruner.getBucketId(count))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid bucket count");
    }

    @ParameterizedTest
    @MethodSource("supportedValues")
    void testSupportedValuesMatchWriter(DataType type, Domain domain, Object value) {
        for (boolean primaryKey : new boolean[] {false, true}) {
            Schema.Builder schema =
                    Schema.newBuilder().column("payload", DataTypes.STRING()).column("key", type);
            if (primaryKey) {
                schema.primaryKey("key");
            }
            TableInfo table =
                    table(
                            schema.build(),
                            Collections.singletonList("key"),
                            Collections.emptyList(),
                            17);
            assertMatchesWriter(
                    table,
                    GenericRow.of(BinaryString.fromString("payload"), value),
                    Collections.singletonMap(new FlussColumnHandle("key", 1), domain));
        }
    }

    @ParameterizedTest
    @EnumSource(value = DataLakeFormat.class, names = "HUDI", mode = EnumSource.Mode.EXCLUDE)
    void testPreconfiguredLakeFormatMatchesWriter(DataLakeFormat format) {
        for (int version : new int[] {1, 2}) {
            for (boolean primaryKey : new boolean[] {false, true}) {
                Schema.Builder schema =
                        Schema.newBuilder()
                                .column("payload", DataTypes.STRING())
                                .column("key", DataTypes.STRING());
                if (primaryKey) {
                    schema.primaryKey("key");
                }
                Configuration config = new Configuration();
                config.set(TABLE_DATALAKE_FORMAT, format);
                config.set(TABLE_KV_FORMAT_VERSION, version);
                TableInfo table =
                        table(
                                schema.build(),
                                Collections.singletonList("key"),
                                Collections.emptyList(),
                                17,
                                config);
                assertThat(table.getTableConfig().isDataLakeEnabled()).isFalse();
                assertMatchesWriter(
                        table,
                        GenericRow.of(
                                BinaryString.fromString("payload"), BinaryString.fromString("北京")),
                        Collections.singletonMap(
                                new FlussColumnHandle("key", 1),
                                Domain.singleValue(VARCHAR, utf8Slice("北京"))));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCompositeKeysAndCustomPrimaryKeyRouting(boolean customBucketKey) {
        Schema schema =
                Schema.newBuilder()
                        .column("payload", DataTypes.STRING())
                        .column("b", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .column("a", DataTypes.STRING())
                        .primaryKey("dt", "a", "b")
                        .build();
        for (DataLakeFormat format : Arrays.asList(null, DataLakeFormat.PAIMON)) {
            for (int version : new int[] {1, 2}) {
                Configuration config = new Configuration();
                if (format != null) {
                    config.set(TABLE_DATALAKE_FORMAT, format);
                }
                config.set(TABLE_KV_FORMAT_VERSION, version);
                TableInfo table =
                        table(
                                schema,
                                customBucketKey
                                        ? Collections.singletonList("b")
                                        : Arrays.asList("a", "b"),
                                Collections.singletonList("dt"),
                                17,
                                config);
                Map<FlussColumnHandle, Domain> domains = new HashMap<>();
                domains.put(new FlussColumnHandle("b", 1), Domain.singleValue(INTEGER, -37L));
                if (!customBucketKey) {
                    domains.put(
                            new FlussColumnHandle("a", 3),
                            Domain.singleValue(VARCHAR, utf8Slice("北京")));
                }
                assertMatchesWriter(
                        table,
                        GenericRow.of(
                                BinaryString.fromString("payload"),
                                -37,
                                BinaryString.fromString("2026-10-20"),
                                BinaryString.fromString("北京")),
                        domains);
            }
        }
    }

    @Test
    void testUnsafeAndNonSingletonDomainsDoNotPrune() {
        TableInfo table =
                table(
                        Schema.newBuilder().column("key", DataTypes.STRING()).build(),
                        Collections.singletonList("key"),
                        Collections.emptyList(),
                        8);
        for (Domain domain :
                Arrays.asList(
                        Domain.all(VARCHAR),
                        Domain.notNull(VARCHAR),
                        Domain.onlyNull(VARCHAR),
                        Domain.singleValue(VARCHAR, utf8Slice("a"), true),
                        Domain.multipleValues(
                                VARCHAR, Arrays.asList(utf8Slice("a"), utf8Slice("b"))),
                        Domain.create(
                                ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("a"))),
                                false),
                        Domain.singleValue(
                                VARCHAR, wrappedBuffer(new byte[] {(byte) 0xc3, 0x28})))) {
            assertThat(
                            FlussBucketPruner.create(
                                    table,
                                    TupleDomain.withColumnDomains(
                                            Collections.singletonMap(
                                                    new FlussColumnHandle("key", 0), domain))))
                    .isEmpty();
        }
        assertThat(FlussBucketPruner.create(table, TupleDomain.none())).isEmpty();
    }

    @Test
    void testWrongBucketColumnIdentityDoesNotPrune() {
        Schema schema =
                Schema.newBuilder()
                        .column("payload", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .build();

        TableInfo table =
                table(schema, Collections.singletonList("id"), Collections.emptyList(), 8);

        assertThat(
                        FlussBucketPruner.create(
                                table,
                                TupleDomain.withColumnDomains(
                                        Collections.singletonMap(
                                                new FlussColumnHandle("id", 0),
                                                Domain.singleValue(INTEGER, 123L)))))
                .isEmpty();

        assertThat(
                        FlussBucketPruner.create(
                                table,
                                TupleDomain.withColumnDomains(
                                        Collections.singletonMap(
                                                new FlussColumnHandle("ID", 1),
                                                Domain.singleValue(INTEGER, 123L)))))
                .isEmpty();
    }

    @Test
    void testTableWithoutBucketKeyDoesNotPrune() {
        TableInfo table =
                table(
                        Schema.newBuilder().column("id", DataTypes.INT()).build(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        8);

        assertThat(table.hasBucketKey()).isFalse();

        assertThat(
                        FlussBucketPruner.create(
                                table,
                                TupleDomain.withColumnDomains(
                                        Collections.singletonMap(
                                                new FlussColumnHandle("id", 0),
                                                Domain.singleValue(INTEGER, 123L)))))
                .isEmpty();
    }

    @Test
    void testIcebergCompositeBucketKeyDoesNotPrune() {
        Configuration config = new Configuration();
        config.set(TABLE_DATALAKE_FORMAT, DataLakeFormat.ICEBERG);
        TableInfo table =
                table(
                        Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.INT())
                                .build(),
                        Arrays.asList("a", "b"),
                        Collections.emptyList(),
                        8,
                        config);
        TupleDomain<FlussColumnHandle> constraint =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                new FlussColumnHandle("a", 0),
                                Domain.singleValue(INTEGER, 1L),
                                new FlussColumnHandle("b", 1),
                                Domain.singleValue(INTEGER, 2L)));

        assertThat(FlussBucketPruner.create(table, constraint)).isEmpty();
    }

    @Test
    void testIcebergSingleBucketKeyStillPrunes() {
        Configuration config = new Configuration();
        config.set(TABLE_DATALAKE_FORMAT, DataLakeFormat.ICEBERG);
        TableInfo table =
                table(
                        Schema.newBuilder().column("a", DataTypes.INT()).build(),
                        Collections.singletonList("a"),
                        Collections.emptyList(),
                        8,
                        config);
        assertMatchesWriter(
                table,
                GenericRow.of(7),
                ImmutableMap.of(new FlussColumnHandle("a", 0), Domain.singleValue(INTEGER, 7L)));
    }

    private static void assertMatchesWriter(
            TableInfo table, GenericRow row, Map<FlussColumnHandle, Domain> domains) {
        KeyEncoder writerEncoder;
        if (table.hasPrimaryKey()) {
            KeyEncoder primaryKeyEncoder =
                    KeyEncoder.ofPrimaryKeyEncoder(
                            table.getRowType(),
                            table.getPhysicalPrimaryKeys(),
                            table.getTableConfig(),
                            table.isDefaultBucketKey());
            writerEncoder =
                    KeyEncoder.ofBucketKeyEncoder(
                            table.getRowType(),
                            table.getBucketKeys(),
                            table.getTableConfig(),
                            table.isDefaultBucketKey(),
                            primaryKeyEncoder);
        } else {
            writerEncoder =
                    KeyEncoder.ofBucketKeyEncoder(
                            table.getRowType(),
                            table.getBucketKeys(),
                            table.getTableConfig().getDataLakeFormat().orElse(null));
        }

        byte[] writerKey = writerEncoder.encodeKey(row);
        FlussBucketPruner pruner =
                FlussBucketPruner.create(table, TupleDomain.withColumnDomains(domains))
                        .orElseThrow(() -> new AssertionError("Expected bucket pruning"));

        int bucketCount = table.isPartitioned() ? 7 : table.getNumBuckets();

        int expectedBucketId =
                BucketingFunction.of(table.getTableConfig().getDataLakeFormat().orElse(null))
                        .bucketing(writerKey, bucketCount);

        assertThat(pruner.getBucketId(bucketCount)).isEqualTo(expectedBucketId);
    }

    private static Stream<Arguments> supportedValues() {
        return Stream.of(
                Arguments.of(DataTypes.BOOLEAN(), Domain.singleValue(BOOLEAN, false), false),
                Arguments.of(DataTypes.BOOLEAN(), Domain.singleValue(BOOLEAN, true), true),
                Arguments.of(DataTypes.TINYINT(), Domain.singleValue(TINYINT, -128L), (byte) -128),
                Arguments.of(DataTypes.TINYINT(), Domain.singleValue(TINYINT, 127L), (byte) 127),
                Arguments.of(
                        DataTypes.SMALLINT(),
                        Domain.singleValue(SMALLINT, -32768L),
                        (short) -32768),
                Arguments.of(
                        DataTypes.SMALLINT(), Domain.singleValue(SMALLINT, 32767L), (short) 32767),
                Arguments.of(
                        DataTypes.INT(),
                        Domain.singleValue(INTEGER, (long) Integer.MIN_VALUE),
                        Integer.MIN_VALUE),
                Arguments.of(
                        DataTypes.INT(),
                        Domain.singleValue(INTEGER, (long) Integer.MAX_VALUE),
                        Integer.MAX_VALUE),
                Arguments.of(
                        DataTypes.BIGINT(),
                        Domain.singleValue(BIGINT, Long.MIN_VALUE),
                        Long.MIN_VALUE),
                Arguments.of(
                        DataTypes.BIGINT(),
                        Domain.singleValue(BIGINT, Long.MAX_VALUE),
                        Long.MAX_VALUE),
                Arguments.of(DataTypes.DATE(), Domain.singleValue(DATE, -1L), -1),
                Arguments.of(DataTypes.DATE(), Domain.singleValue(DATE, 20016L), 20016),
                Arguments.of(
                        DataTypes.STRING(),
                        Domain.singleValue(VARCHAR, utf8Slice("")),
                        BinaryString.fromString("")),
                Arguments.of(
                        DataTypes.STRING(),
                        Domain.singleValue(VARCHAR, utf8Slice("北京 😀 ")),
                        BinaryString.fromString("北京 😀 ")),
                Arguments.of(
                        DataTypes.BYTES(),
                        Domain.singleValue(VARBINARY, wrappedBuffer(new byte[] {0, -1, 127})),
                        new byte[] {0, -1, 127}),
                Arguments.of(
                        DataTypes.BYTES(),
                        Domain.singleValue(VARBINARY, wrappedBuffer(new byte[0])),
                        new byte[0]));
    }

    private static TableInfo table(
            Schema schema, List<String> bucketKeys, List<String> partitionKeys, int count) {
        return table(schema, bucketKeys, partitionKeys, count, new Configuration());
    }

    private static TableInfo table(
            Schema schema,
            List<String> bucketKeys,
            List<String> partitionKeys,
            int count,
            Configuration config) {
        return new TableInfo(
                TablePath.of("db", "t"),
                TABLE_ID,
                0,
                schema,
                bucketKeys,
                partitionKeys,
                count,
                config,
                new Configuration(),
                null,
                null,
                0L,
                0L);
    }
}
