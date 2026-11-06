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

import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.fluss.trino.TestingFlussMetadata.tableInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests partition extraction and native-value conversion without a Fluss client. */
final class FlussPartitionPrunerTest {
    @ParameterizedTest
    @MethodSource("supportedValues")
    void testSupportedSingleValues(DataType type, Domain domain, String expected) {
        assertThat(extractValue(type, domain))
                .contains(new PartitionSpec(Collections.singletonMap("p", expected)));
    }

    @ParameterizedTest
    @MethodSource("unsupportedValues")
    void testUnsupportedTypesAndMismatchedDomains(DataType type, Domain domain) {
        assertThat(extractValue(type, domain)).isEmpty();
    }

    @Test
    void testNonSingletonAndNullableDomainsDoNotPrune() {
        for (Domain domain :
                Arrays.asList(
                        Domain.all(VARCHAR),
                        Domain.notNull(VARCHAR),
                        Domain.onlyNull(VARCHAR),
                        Domain.singleValue(VARCHAR, utf8Slice("east"), true),
                        Domain.multipleValues(
                                VARCHAR, Arrays.asList(utf8Slice("east"), utf8Slice("west"))),
                        Domain.create(
                                ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("east"))),
                                false))) {
            assertThat(extractValue(DataTypes.STRING(), domain)).isEmpty();
        }
    }

    @Test
    void testPartialSpecRetainsOnlySupportedPartitionConstraints() {
        TableInfo table =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("Region", DataTypes.STRING())
                                                .column("category", DataTypes.INT())
                                                .column("code", DataTypes.CHAR(4))
                                                .build())
                                .partitionedBy("category", "Region", "code")
                                .distributedBy(1, "id")
                                .build());
        Map<FlussColumnHandle, Domain> domains = new LinkedHashMap<>();
        domains.put(new FlussColumnHandle("id", 0), Domain.singleValue(INTEGER, 10L));
        domains.put(
                new FlussColumnHandle("Region", 1), Domain.singleValue(VARCHAR, utf8Slice("east")));
        domains.put(new FlussColumnHandle("category", 2), Domain.singleValue(INTEGER, 7L));
        domains.put(
                new FlussColumnHandle("code", 3),
                Domain.singleValue(createCharType(4), utf8Slice("a")));
        TupleDomain<FlussColumnHandle> constraint = TupleDomain.withColumnDomains(domains);
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("Region", "east");
        expected.put("category", "7");
        assertThat(FlussPartitionPruner.extractPartitionSpec(table, constraint))
                .contains(new PartitionSpec(expected));
        assertThat(constraint.getDomains().get()).hasSize(4);

        domains.put(
                new FlussColumnHandle("category", 2),
                Domain.multipleValues(INTEGER, Arrays.asList(7L, 8L)));
        assertThat(
                        FlussPartitionPruner.extractPartitionSpec(
                                table, TupleDomain.withColumnDomains(domains)))
                .contains(new PartitionSpec(Collections.singletonMap("Region", "east")));
    }

    @Test
    void testOnlyExactPartitionColumnHandlesAreUsed() {
        TableInfo table = partitionedTable(DataTypes.STRING());
        for (FlussColumnHandle column :
                Arrays.asList(
                        new FlussColumnHandle("id", 0),
                        new FlussColumnHandle("P", 1),
                        new FlussColumnHandle("p", 0))) {
            assertThat(
                            FlussPartitionPruner.extractPartitionSpec(
                                    table,
                                    TupleDomain.withColumnDomains(
                                            Collections.singletonMap(
                                                    column,
                                                    Domain.singleValue(
                                                            VARCHAR, utf8Slice("east"))))))
                    .isEmpty();
        }
    }

    @Test
    void testUnconstrainedAndNonPartitionedTables() {
        assertThat(
                        FlussPartitionPruner.extractPartitionSpec(
                                partitionedTable(DataTypes.STRING()), TupleDomain.all()))
                .isEmpty();
        TableInfo table =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                                .distributedBy(1, "id")
                                .build());
        assertThat(
                        FlussPartitionPruner.extractPartitionSpec(
                                table,
                                TupleDomain.withColumnDomains(
                                        Collections.singletonMap(
                                                new FlussColumnHandle("id", 0),
                                                Domain.singleValue(INTEGER, 7L)))))
                .isEmpty();
    }

    @Test
    void testNoneMustBeHandledBeforePartitionDiscovery() {
        assertThatThrownBy(
                        () ->
                                FlussPartitionPruner.extractPartitionSpec(
                                        partitionedTable(DataTypes.STRING()), TupleDomain.none()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("none");
    }

    private static Optional<PartitionSpec> extractValue(DataType type, Domain domain) {
        return FlussPartitionPruner.extractPartitionSpec(
                partitionedTable(type),
                TupleDomain.withColumnDomains(
                        Collections.singletonMap(new FlussColumnHandle("p", 1), domain)));
    }

    private static TableInfo partitionedTable(DataType type) {
        return tableInfo(
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("p", type)
                                        .build())
                        .partitionedBy("p")
                        .distributedBy(1, "id")
                        .build());
    }

    private static Stream<Arguments> supportedValues() {
        return Stream.of(
                Arguments.of(
                        DataTypes.STRING(), Domain.singleValue(VARCHAR, utf8Slice("北京")), "北京"),
                Arguments.of(
                        DataTypes.STRING(),
                        Domain.singleValue(VARCHAR, utf8Slice("east ")),
                        "east "),
                Arguments.of(DataTypes.BOOLEAN(), Domain.singleValue(BOOLEAN, true), "true"),
                Arguments.of(DataTypes.BOOLEAN(), Domain.singleValue(BOOLEAN, false), "false"),
                Arguments.of(DataTypes.TINYINT(), Domain.singleValue(TINYINT, -128L), "-128"),
                Arguments.of(DataTypes.TINYINT(), Domain.singleValue(TINYINT, 127L), "127"),
                Arguments.of(DataTypes.SMALLINT(), Domain.singleValue(SMALLINT, -32768L), "-32768"),
                Arguments.of(DataTypes.SMALLINT(), Domain.singleValue(SMALLINT, 32767L), "32767"),
                Arguments.of(
                        DataTypes.INT(),
                        Domain.singleValue(INTEGER, (long) Integer.MIN_VALUE),
                        "-2147483648"),
                Arguments.of(
                        DataTypes.INT(),
                        Domain.singleValue(INTEGER, (long) Integer.MAX_VALUE),
                        "2147483647"),
                Arguments.of(
                        DataTypes.BIGINT(),
                        Domain.singleValue(BIGINT, Long.MIN_VALUE),
                        "-9223372036854775808"),
                Arguments.of(
                        DataTypes.BIGINT(),
                        Domain.singleValue(BIGINT, Long.MAX_VALUE),
                        "9223372036854775807"),
                Arguments.of(DataTypes.DATE(), Domain.singleValue(DATE, -1L), "1969-12-31"),
                Arguments.of(DataTypes.DATE(), Domain.singleValue(DATE, 0L), "1970-01-01"),
                Arguments.of(DataTypes.DATE(), Domain.singleValue(DATE, 19782L), "2024-02-29"));
    }

    private static Stream<Arguments> unsupportedValues() {
        return Stream.of(
                Arguments.of(
                        DataTypes.CHAR(4), Domain.singleValue(createCharType(4), utf8Slice("a"))),
                Arguments.of(
                        DataTypes.DECIMAL(10, 2),
                        Domain.singleValue(createDecimalType(10, 2), 123L)),
                Arguments.of(DataTypes.DOUBLE(), Domain.singleValue(DOUBLE, 1.0)),
                Arguments.of(DataTypes.TIMESTAMP(3), Domain.singleValue(TIMESTAMP_MILLIS, 0L)),
                Arguments.of(DataTypes.BYTES(), Domain.singleValue(VARBINARY, utf8Slice("a"))),
                Arguments.of(DataTypes.STRING(), Domain.singleValue(BIGINT, 1L)),
                Arguments.of(DataTypes.BOOLEAN(), Domain.singleValue(BIGINT, 1L)),
                Arguments.of(DataTypes.TINYINT(), Domain.singleValue(BIGINT, 1L)),
                Arguments.of(DataTypes.SMALLINT(), Domain.singleValue(BIGINT, 1L)),
                Arguments.of(DataTypes.INT(), Domain.singleValue(BIGINT, 1L)),
                Arguments.of(DataTypes.BIGINT(), Domain.singleValue(INTEGER, 1L)),
                Arguments.of(DataTypes.DATE(), Domain.singleValue(INTEGER, 0L)));
    }
}
