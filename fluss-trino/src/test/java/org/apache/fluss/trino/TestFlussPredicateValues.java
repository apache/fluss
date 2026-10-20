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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableMap;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;

import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.fluss.trino.FlussPartitionPruner.extractPartitionSpec;
import static org.apache.fluss.trino.FlussPredicateValues.toFlussValue;
import static org.assertj.core.api.Assertions.assertThat;

class TestFlussPredicateValues {

    @Test
    void testSupportedIntegralValues() {
        assertThat(toFlussValue(DataTypeRoot.TINYINT, Domain.singleValue(TINYINT, 127L)))
                .contains((byte) 127);
        assertThat(toFlussValue(DataTypeRoot.SMALLINT, Domain.singleValue(SMALLINT, -32768L)))
                .contains((short) -32768);
        assertThat(toFlussValue(DataTypeRoot.INTEGER, Domain.singleValue(INTEGER, 123L)))
                .contains(123);
        assertThat(toFlussValue(DataTypeRoot.BIGINT, Domain.singleValue(BIGINT, Long.MAX_VALUE)))
                .contains(Long.MAX_VALUE);
        assertThat(toFlussValue(DataTypeRoot.DATE, Domain.singleValue(DATE, 20_000L)))
                .contains(20_000);
    }

    @Test
    void testBooleanValues() {
        assertThat(toFlussValue(DataTypeRoot.BOOLEAN, Domain.singleValue(BOOLEAN, true)))
                .contains(true);
        assertThat(toFlussValue(DataTypeRoot.BOOLEAN, Domain.singleValue(BOOLEAN, false)))
                .contains(false);
    }

    @Test
    void testLosslessStringConversion() {
        Optional<Object> value =
                toFlussValue(
                        DataTypeRoot.STRING,
                        Domain.singleValue(VARCHAR, Slices.utf8Slice("Fluss-中文-🙂")));
        assertThat(value).isPresent();
        assertThat(value.get().toString()).isEqualTo("Fluss-中文-🙂");
    }

    @Test
    void testInvalidUtf8FallsBack() {
        byte[] invalidUtf8 = {(byte) 0xC3, (byte) 0x28};
        assertThat(
                        toFlussValue(
                                DataTypeRoot.STRING,
                                Domain.singleValue(VARCHAR, Slices.wrappedBuffer(invalidUtf8))))
                .isEmpty();
    }

    @Test
    void testBytesPreserveBinaryContents() {
        byte[] bytes = {0, 1, (byte) 0x80, (byte) 0xFF};
        Optional<Object> converted =
                toFlussValue(
                        DataTypeRoot.BYTES,
                        Domain.singleValue(VARBINARY, Slices.wrappedBuffer(bytes)));
        assertThat(converted).isPresent();
        assertThat((byte[]) converted.get()).containsExactly(bytes);
    }

    @Test
    void testTypeMismatchFallsBack() {
        assertThat(toFlussValue(DataTypeRoot.INTEGER, Domain.singleValue(BIGINT, 123L))).isEmpty();
        assertThat(toFlussValue(DataTypeRoot.BIGINT, Domain.singleValue(INTEGER, 123L))).isEmpty();
        assertThat(
                        toFlussValue(
                                DataTypeRoot.STRING,
                                Domain.singleValue(VARBINARY, Slices.utf8Slice("123"))))
                .isEmpty();
        assertThat(
                        toFlussValue(
                                DataTypeRoot.BYTES,
                                Domain.singleValue(VARCHAR, Slices.utf8Slice("123"))))
                .isEmpty();
    }

    @Test
    void testUnsupportedAndNonSingleDomainsFallBack() {
        assertThat(toFlussValue(DataTypeRoot.INTEGER, Domain.onlyNull(INTEGER))).isEmpty();
        assertThat(
                        toFlussValue(
                                DataTypeRoot.INTEGER,
                                Domain.multipleValues(INTEGER, Arrays.asList(1L, 2L))))
                .isEmpty();
        assertThat(
                        toFlussValue(
                                DataTypeRoot.INTEGER,
                                Domain.create(
                                        ValueSet.ofRanges(Range.greaterThan(INTEGER, 10L)), false)))
                .isEmpty();
        assertThat(
                        toFlussValue(
                                DataTypeRoot.FLOAT,
                                Domain.singleValue(
                                        io.trino.spi.type.RealType.REAL,
                                        (long) Float.floatToRawIntBits(1.5f))))
                .isEmpty();
    }

    @Test
    void testIntegralBoundaryValues() {
        assertThat(
                        toFlussValue(
                                DataTypeRoot.TINYINT,
                                Domain.singleValue(TINYINT, (long) Byte.MIN_VALUE)))
                .contains(Byte.MIN_VALUE);
        assertThat(
                        toFlussValue(
                                DataTypeRoot.TINYINT,
                                Domain.singleValue(TINYINT, (long) Byte.MAX_VALUE)))
                .contains(Byte.MAX_VALUE);
        assertThat(
                        toFlussValue(
                                DataTypeRoot.SMALLINT,
                                Domain.singleValue(SMALLINT, (long) Short.MIN_VALUE)))
                .contains(Short.MIN_VALUE);
        assertThat(
                        toFlussValue(
                                DataTypeRoot.SMALLINT,
                                Domain.singleValue(SMALLINT, (long) Short.MAX_VALUE)))
                .contains(Short.MAX_VALUE);
        assertThat(
                        toFlussValue(
                                DataTypeRoot.INTEGER,
                                Domain.singleValue(INTEGER, (long) Integer.MIN_VALUE)))
                .contains(Integer.MIN_VALUE);
        assertThat(
                        toFlussValue(
                                DataTypeRoot.INTEGER,
                                Domain.singleValue(INTEGER, (long) Integer.MAX_VALUE)))
                .contains(Integer.MAX_VALUE);

        assertThat(toFlussValue(DataTypeRoot.DATE, Domain.singleValue(DATE, -1L))).contains(-1);
        assertThat(toFlussValue(DataTypeRoot.DATE, Domain.singleValue(DATE, 20_000L)))
                .contains(20_000);
    }

    @Test
    void testPartitionPruningStillWorks() {
        Schema schema =
                Schema.newBuilder()
                        .column("dt", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .build();
        TableInfo table = partitionedTable(schema);
        TupleDomain<FlussColumnHandle> constraint =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                new FlussColumnHandle("dt", 0),
                                Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-10-20")),
                                new FlussColumnHandle("id", 1),
                                Domain.singleValue(INTEGER, 10L)));

        Optional<PartitionSpec> spec = extractPartitionSpec(table, constraint);
        assertThat(spec).isPresent();
        assertThat(spec.get().getSpecMap())
                .containsExactlyInAnyOrderEntriesOf(Collections.singletonMap("dt", "2026-10-20"));
    }

    @Test
    void testInvalidUtf8DoesNotPrunePartition() {
        Schema schema =
                Schema.newBuilder()
                        .column("dt", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .build();
        TableInfo table = partitionedTable(schema);
        TupleDomain<FlussColumnHandle> constraint =
                TupleDomain.withColumnDomains(
                        Collections.singletonMap(
                                new FlussColumnHandle("dt", 0),
                                Domain.singleValue(
                                        VARCHAR, Slices.wrappedBuffer((byte) 0xC3, (byte) 0x28))));
        assertThat(extractPartitionSpec(table, constraint)).isEmpty();
    }

    @Test
    void testBytesDoNotEnablePartitionPruning() {
        Schema schema =
                Schema.newBuilder()
                        .column("dt", DataTypes.BYTES())
                        .column("id", DataTypes.INT())
                        .build();
        TableInfo table = partitionedTable(schema);
        TupleDomain<FlussColumnHandle> constraint =
                TupleDomain.withColumnDomains(
                        Collections.singletonMap(
                                new FlussColumnHandle("dt", 0),
                                Domain.singleValue(
                                        VARBINARY, Slices.wrappedBuffer(new byte[] {1, 2, 3}))));
        assertThat(extractPartitionSpec(table, constraint)).isEmpty();
    }

    private static TableInfo partitionedTable(Schema schema) {
        return new TableInfo(
                TablePath.of("db", "t"),
                42L,
                0,
                schema,
                Collections.singletonList("id"),
                Collections.singletonList("dt"),
                4,
                new Configuration(),
                new Configuration(),
                null,
                null,
                0L,
                0L);
    }
}
