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

import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableMap;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.FeaturesConfig;
import io.trino.block.BlockJsonSerde;
import io.trino.metadata.TypeRegistry;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests table handle identity, constraints and serialization. */
final class FlussTableHandleTest {
    @Test
    void testWithConstraintPreservesIdentityAndOriginalHandle() {
        FlussTableHandle original =
                new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, TupleDomain.all());
        FlussTableHandle filtered = original.withConstraint(TupleDomain.none());
        assertThat(filtered).isNotEqualTo(original);
        assertThat(filtered.withConstraint(TupleDomain.all())).isEqualTo(original);
        assertThat(original.getConstraint().isAll()).isTrue();
        assertThat(filtered.getConstraint().isNone()).isTrue();
        assertThatThrownBy(() -> original.withConstraint(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("constraint");
    }

    @Test
    void testJsonRoundTrip() {
        JsonCodec<FlussTableHandle> codec = createHandleCodec();
        Map<FlussColumnHandle, Domain> domains = new LinkedHashMap<>();
        domains.put(
                new FlussColumnHandle("id", 0),
                Domain.multipleValues(BIGINT, Arrays.asList(1L, 7L)));
        domains.put(
                new FlussColumnHandle("name", 1), Domain.singleValue(VARCHAR, utf8Slice("Alice")));
        domains.put(
                new FlussColumnHandle("date", 2),
                Domain.create(ValueSet.ofRanges(Range.range(DATE, 1L, true, 100L, false)), true));
        domains.put(new FlussColumnHandle("flag", 3), Domain.onlyNull(BOOLEAN));
        domains.put(
                new FlussColumnHandle("amount", 4),
                Domain.singleValue(createDecimalType(10, 2), 1234L));
        domains.put(new FlussColumnHandle("other", 5), Domain.notNull(VARCHAR));

        for (TupleDomain<FlussColumnHandle> constraint :
                Arrays.asList(
                        TupleDomain.<FlussColumnHandle>all(),
                        TupleDomain.<FlussColumnHandle>none(),
                        TupleDomain.withColumnDomains(domains))) {
            FlussTableHandle handle =
                    new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, constraint);

            FlussTableHandle decoded = codec.fromJson(codec.toJson(handle));

            assertThat(decoded).isEqualTo(handle).hasSameHashCodeAs(handle);
        }
    }

    @Test
    void testEqualityIncludesLogicalAndPhysicalIdentity() {
        FlussTableHandle handle =
                new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, TupleDomain.all());
        FlussTableHandle same =
                new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, TupleDomain.all());
        assertThat(handle).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle(
                                "other", "users", "Sales", "Users", 42, 3, TupleDomain.all()));
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle(
                                "sales", "other", "Sales", "Users", 42, 3, TupleDomain.all()));
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle(
                                "sales", "users", "sales", "Users", 42, 3, TupleDomain.all()));
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle(
                                "sales", "users", "Sales", "users", 42, 3, TupleDomain.all()));
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle(
                                "sales", "users", "Sales", "Users", 43, 3, TupleDomain.all()));
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle(
                                "sales", "users", "Sales", "Users", 42, 4, TupleDomain.all()));
    }

    @Test
    void testRejectNegativeIdentifiers() {
        assertThatThrownBy(
                        () ->
                                new FlussTableHandle(
                                        "sales",
                                        "users",
                                        "Sales",
                                        "Users",
                                        -1,
                                        0,
                                        TupleDomain.all()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tableId");
        assertThatThrownBy(
                        () ->
                                new FlussTableHandle(
                                        "sales",
                                        "users",
                                        "Sales",
                                        "Users",
                                        0,
                                        -1,
                                        TupleDomain.all()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schemaId");
    }

    @Test
    void testRejectMissingPhysicalName() {
        assertThatThrownBy(
                        () ->
                                new FlussTableHandle(
                                        "sales", "users", null, "Users", 42, 3, TupleDomain.all()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("flussDatabaseName");
        assertThatThrownBy(
                        () ->
                                new FlussTableHandle(
                                        "sales", "users", "Sales", null, 42, 3, TupleDomain.all()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("flussTableName");
    }

    private static JsonCodec<FlussTableHandle> createHandleCodec() {
        TypeRegistry types = new TypeRegistry(new TypeOperators(), new FeaturesConfig());
        JsonMapperProvider mapper = new JsonMapperProvider();

        mapper.setJsonDeserializers(
                ImmutableMap.of(
                        Type.class,
                        new TypeDeserializer(types::getType),
                        Block.class,
                        new BlockJsonSerde.Deserializer(TESTING_BLOCK_ENCODING_SERDE)));
        mapper.setJsonSerializers(
                Collections.singletonMap(
                        Block.class, new BlockJsonSerde.Serializer(TESTING_BLOCK_ENCODING_SERDE)));

        return new JsonCodecFactory(mapper).jsonCodec(FlussTableHandle.class);
    }
}
