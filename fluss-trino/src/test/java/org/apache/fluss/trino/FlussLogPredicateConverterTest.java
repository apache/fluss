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

package org.apache.fluss.trino;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.messages.PbPredicate;
import org.apache.fluss.rpc.util.PredicateMessageUtils;
import org.apache.fluss.types.DataTypes;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.fluss.trino.TestingFlussMetadata.tableInfo;
import static org.assertj.core.api.Assertions.assertThat;

/** Verifies batch predicates against the full schema and exact Trino range bounds. */
final class FlussLogPredicateConverterTest {
    private static final TableInfo TABLE =
            tableInfo(
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("id", DataTypes.INT())
                                            .column("amount", DataTypes.INT())
                                            .column("label", DataTypes.STRING())
                                            .column("flag", DataTypes.BOOLEAN())
                                            .column("day", DataTypes.DATE())
                                            .build())
                            .distributedBy(1)
                            .property(
                                    ConfigOptions.TABLE_STATISTICS_COLUMNS, "amount,label,flag,day")
                            .build());

    @Test
    void testDisjointRangesAndCompactionUseFullSchemaOrdinal() {
        Domain domain =
                Domain.create(
                        ValueSet.ofRanges(
                                Range.range(INTEGER, 10L, false, 20L, true),
                                Range.greaterThanOrEqual(INTEGER, 30L)),
                        false);
        Predicate exact = convert(domain, 256);
        Predicate compacted = convert(domain, 1);
        assertThat(exact.test(GenericRow.of(99, 10))).isFalse();
        assertThat(exact.test(GenericRow.of(99, 20))).isTrue();
        assertThat(exact.test(GenericRow.of(99, 25))).isFalse();
        assertThat(exact.test(GenericRow.of(99, 30))).isTrue();
        assertThat(compacted.test(GenericRow.of(99, 25))).isTrue();
        assertThat(compacted.test(GenericRow.of(99, 10))).isFalse();
    }

    @Test
    void testLargeRangeUnionHasShallowRpcTree() {
        Domain excluded =
                Domain.multipleValues(
                        INTEGER,
                        LongStream.range(0, 2048)
                                .map(value -> value * 2)
                                .boxed()
                                .collect(Collectors.toList()));
        Predicate predicate =
                convert(Domain.create(excluded.getValues().complement(), false), Integer.MAX_VALUE);
        assertThat(((CompoundPredicate) predicate).children()).hasSize(2049);
        PbPredicate decoded = new PbPredicate();
        decoded.parseFrom(
                PredicateMessageUtils.toPbPredicate(predicate, TABLE.getRowType()).toByteArray());
        assertThat(PredicateMessageUtils.toPredicate(decoded, TABLE.getRowType()))
                .isEqualTo(predicate);
    }

    @Test
    void testDefaultCompactionBoundary() {
        int threshold = FlussSessionProperties.DEFAULT_DOMAIN_COMPACTION_THRESHOLD;
        for (int size : Arrays.asList(256, 257)) {
            Domain values =
                    Domain.multipleValues(
                            INTEGER,
                            LongStream.range(0, size)
                                    .map(value -> value * 2)
                                    .boxed()
                                    .collect(Collectors.toList()));
            Predicate predicate = convert(values, threshold);
            assertThat(predicate.test(GenericRow.of(0, 0))).isTrue();
            assertThat(predicate.test(GenericRow.of(0, 1))).isEqualTo(size == 257);
        }
    }

    @Test
    void testStatisticsPredicatesForStringBooleanAndDate() {
        Map<FlussColumnHandle, Domain> domains = new LinkedHashMap<>();
        domains.put(new FlussColumnHandle("id", 0), Domain.singleValue(INTEGER, 1L));
        domains.put(
                new FlussColumnHandle("label", 2),
                Domain.multipleValues(VARCHAR, Arrays.asList(utf8Slice("a"), utf8Slice("z"))));
        domains.put(new FlussColumnHandle("flag", 3), Domain.singleValue(BOOLEAN, true));
        domains.put(
                new FlussColumnHandle("day", 4),
                Domain.create(ValueSet.ofRanges(Range.lessThan(DATE, 20L)), false));
        Predicate predicate =
                FlussLogPredicateConverter.convert(
                                TABLE, TupleDomain.withColumnDomains(domains), 256)
                        .get();
        assertThat(batchMayMatch(predicate, "z", true, 19)).isTrue();
        assertThat(batchMayMatch(predicate, "m", true, 19)).isFalse();
        assertThat(batchMayMatch(predicate, "z", false, 19)).isFalse();
        assertThat(batchMayMatch(predicate, "z", true, 20)).isFalse();
    }

    private static boolean batchMayMatch(Predicate predicate, String label, boolean flag, int day) {
        GenericRow stats = GenericRow.of(null, null, BinaryString.fromString(label), flag, day);
        return predicate.test(1, stats, stats, new int[] {-1, -1, 0, 0, 0});
    }

    @Test
    void testNullableOrInvalidDisjunctFallsBack() {
        assertThat(
                        FlussLogPredicateConverter.convert(
                                TABLE, constraint(Domain.singleValue(INTEGER, 1L, true)), 256))
                .isEmpty();
        Domain labels =
                Domain.multipleValues(
                        VARCHAR,
                        Arrays.asList(utf8Slice("valid"), wrappedBuffer(new byte[] {(byte) 0xff})));
        assertThat(
                        FlussLogPredicateConverter.convert(
                                TABLE,
                                TupleDomain.withColumnDomains(
                                        Collections.singletonMap(
                                                new FlussColumnHandle("label", 2), labels)),
                                256))
                .isEmpty();
    }

    @Test
    void testUnindexedColumnDoesNotPushDown() {
        assertThat(
                        FlussLogPredicateConverter.convert(
                                TABLE,
                                TupleDomain.withColumnDomains(
                                        Collections.singletonMap(
                                                new FlussColumnHandle("id", 0),
                                                Domain.singleValue(INTEGER, 1L))),
                                256))
                .isEmpty();
    }

    private static Predicate convert(Domain domain, int threshold) {
        return FlussLogPredicateConverter.convert(TABLE, constraint(domain), threshold).get();
    }

    private static TupleDomain<FlussColumnHandle> constraint(Domain amount) {
        Map<FlussColumnHandle, Domain> domains = new LinkedHashMap<>();
        domains.put(new FlussColumnHandle("id", 0), Domain.singleValue(INTEGER, 1L));
        domains.put(new FlussColumnHandle("amount", 1), amount);
        return TupleDomain.withColumnDomains(domains);
    }
}
