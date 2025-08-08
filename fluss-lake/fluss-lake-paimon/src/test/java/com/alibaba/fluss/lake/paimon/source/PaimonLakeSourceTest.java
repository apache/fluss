/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.source;

import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.predicate.FieldRef;
import com.alibaba.fluss.predicate.FunctionVisitor;
import com.alibaba.fluss.predicate.LeafFunction;
import com.alibaba.fluss.predicate.LeafPredicate;
import com.alibaba.fluss.predicate.Predicate;
import com.alibaba.fluss.predicate.PredicateBuilder;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.paimon.schema.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** UT for {@link PaimonLakeSource}. */
class PaimonLakeSourceTest extends PaimonSourceTestBase {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                    .column("name", org.apache.paimon.types.DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final PredicateBuilder FLUSS_BUILDER =
            new PredicateBuilder(RowType.of(DataTypes.BIGINT(), DataTypes.STRING()));

    @BeforeAll
    protected static void beforeAll() {
        PaimonSourceTestBase.beforeAll();
    }

    @Test
    void testWithFilters() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_filters");
        createTable(tablePath, SCHEMA);

        // test all filter can be accepted
        Predicate filter1 = FLUSS_BUILDER.equal(0, 123L);
        Predicate filter2 = FLUSS_BUILDER.isNotNull(1);
        List<Predicate> allFilters = Arrays.asList(filter1, filter2);

        LakeSource<?> lakeSource = lakeStorage.createLakeSource(tablePath);

        LakeSource.FilterPushDownResult filterPushDownResult = lakeSource.withFilters(allFilters);

        assertThat(filterPushDownResult.acceptedPredicates()).isEqualTo(allFilters);
        assertThat(filterPushDownResult.remainingPredicates()).isEmpty();

        // test mix one unaccepted filter
        Predicate nonConvertibleFilter =
                new LeafPredicate(
                        new UnSupportFilterFunction(),
                        DataTypes.INT(),
                        0,
                        "f1",
                        Collections.emptyList());
        allFilters = Arrays.asList(nonConvertibleFilter, filter1, filter2);

        filterPushDownResult = lakeSource.withFilters(allFilters);
        assertThat(filterPushDownResult.acceptedPredicates().toString())
                .isEqualTo(Arrays.asList(filter1, filter2).toString());
        assertThat(filterPushDownResult.remainingPredicates().toString())
                .isEqualTo(Collections.singleton(nonConvertibleFilter).toString());

        // test all are unaccepted filter
        allFilters = Arrays.asList(nonConvertibleFilter, nonConvertibleFilter);
        filterPushDownResult = lakeSource.withFilters(allFilters);
        assertThat(filterPushDownResult.acceptedPredicates()).isEmpty();
        assertThat(filterPushDownResult.remainingPredicates().toString())
                .isEqualTo(allFilters.toString());
    }

    private static class UnSupportFilterFunction extends LeafFunction {

        @Override
        public boolean test(DataType type, Object field, List<Object> literals) {
            return false;
        }

        @Override
        public boolean test(
                DataType type,
                long rowCount,
                Object min,
                Object max,
                Long nullCount,
                List<Object> literals) {
            return false;
        }

        @Override
        public Optional<LeafFunction> negate() {
            return Optional.empty();
        }

        @Override
        public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
            throw new UnsupportedOperationException(
                    "Unsupported filter function for test purpose.");
        }
    }
}
