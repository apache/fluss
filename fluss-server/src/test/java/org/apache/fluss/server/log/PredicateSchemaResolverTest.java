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

package org.apache.fluss.server.log;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PredicateSchemaResolver}. */
class PredicateSchemaResolverTest {

    private static final int SCHEMA_ID_1 = 1;
    private static final int SCHEMA_ID_2 = 2;

    // Schema v1: columns (a INT colId=1, b STRING colId=2)
    private static final Schema SCHEMA_V1 =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("a", DataTypes.INT(), null, 1),
                                    new Schema.Column("b", DataTypes.STRING(), null, 2)))
                    .build();

    private static final RowType ROW_TYPE_V1 =
            DataTypes.ROW(
                    DataTypes.FIELD("a", DataTypes.INT()),
                    DataTypes.FIELD("b", DataTypes.STRING()));

    // Schema v2: columns (a INT, b STRING, c STRING) with column IDs 1, 2, 3
    // Simulates adding a new column "c"
    private static final Schema SCHEMA_V2 =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("a", DataTypes.INT(), null, 1),
                                    new Schema.Column("b", DataTypes.STRING(), null, 2),
                                    new Schema.Column("c", DataTypes.STRING(), null, 3)))
                    .build();

    @Test
    void testSameSchemaIdReturnsSamePredicate() {
        Predicate predicate = new PredicateBuilder(ROW_TYPE_V1).equal(0, 42);
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID_1, SCHEMA_V1);

        PredicateSchemaResolver resolver =
                new PredicateSchemaResolver(predicate, SCHEMA_ID_1, schemaGetter);

        Predicate result = resolver.resolve(SCHEMA_ID_1);
        assertThat(result).isSameAs(predicate);
    }

    @Test
    void testNegativePredicateSchemaIdAlwaysReturnsOriginal() {
        Predicate predicate = new PredicateBuilder(ROW_TYPE_V1).equal(0, 42);
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID_1, SCHEMA_V1);

        // Negative predicate schema ID means schema tracking is disabled
        PredicateSchemaResolver resolver = new PredicateSchemaResolver(predicate, -1, schemaGetter);

        // Should return original predicate regardless of batch schema ID
        assertThat(resolver.resolve(SCHEMA_ID_1)).isSameAs(predicate);
        assertThat(resolver.resolve(SCHEMA_ID_2)).isSameAs(predicate);
        assertThat(resolver.resolve(999)).isSameAs(predicate);
    }

    @Test
    void testCacheHitReturnsSameResult() {
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID_1, SCHEMA_V1);
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(SCHEMA_V2, SCHEMA_ID_2));

        Predicate predicate = new PredicateBuilder(ROW_TYPE_V1).equal(0, 42);
        PredicateSchemaResolver resolver =
                new PredicateSchemaResolver(predicate, SCHEMA_ID_1, schemaGetter);

        // First call populates cache
        Predicate first = resolver.resolve(SCHEMA_ID_2);
        // Second call should return the same cached instance
        Predicate second = resolver.resolve(SCHEMA_ID_2);

        assertThat(first).isNotNull();
        assertThat(second).isSameAs(first);
    }

    @Test
    void testSchemaGetterThrowsInConstructor() {
        // SchemaGetter that throws for the predicate schema ID
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter();

        Predicate predicate = new PredicateBuilder(ROW_TYPE_V1).equal(0, 42);

        // Constructor should NOT throw even though getSchema will throw
        PredicateSchemaResolver resolver =
                new PredicateSchemaResolver(predicate, SCHEMA_ID_1, schemaGetter);

        // Same schema still works (fast path doesn't need predicateSchema)
        assertThat(resolver.resolve(SCHEMA_ID_1)).isSameAs(predicate);
        // Different schema returns null because predicateSchema couldn't be resolved
        assertThat(resolver.resolve(SCHEMA_ID_2)).isNull();
    }

    @Test
    void testCrossSchemaAdaptation() {
        // Predicate built on schema v1 (a=0, b=1), predicate on column "a" at index 0
        Predicate predicate = new PredicateBuilder(ROW_TYPE_V1).equal(0, 42);

        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID_1, SCHEMA_V1);
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(SCHEMA_V2, SCHEMA_ID_2));

        PredicateSchemaResolver resolver =
                new PredicateSchemaResolver(predicate, SCHEMA_ID_1, schemaGetter);

        // Resolve for schema v2 which has an extra column "c"
        Predicate adapted = resolver.resolve(SCHEMA_ID_2);

        // The adapted predicate should still reference column "a" at index 0 in schema v2
        // because column IDs match (both have column ID 1 for "a" at index 0)
        assertThat(adapted).isNotNull();
        assertThat(adapted).isInstanceOf(LeafPredicate.class);
        LeafPredicate leaf = (LeafPredicate) adapted;
        assertThat(leaf.index()).isEqualTo(0);
        assertThat(leaf.fieldName()).isEqualTo("a");
    }

    @Test
    void testCrossSchemaAdaptationWithReorderedColumns() {
        // Schema v1: (a INT colId=1, b STRING colId=2)
        // Schema v3: (b STRING colId=2, a INT colId=1) — columns reordered
        Schema schemaV3 =
                Schema.newBuilder()
                        .fromColumns(
                                Arrays.asList(
                                        new Schema.Column("b", DataTypes.STRING(), null, 2),
                                        new Schema.Column("a", DataTypes.INT(), null, 1)))
                        .build();
        int schemaId3 = 3;

        // Predicate on column "a" at index 0 in schema v1
        Predicate predicate = new PredicateBuilder(ROW_TYPE_V1).equal(0, 42);

        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID_1, SCHEMA_V1);
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(schemaV3, schemaId3));

        PredicateSchemaResolver resolver =
                new PredicateSchemaResolver(predicate, SCHEMA_ID_1, schemaGetter);

        Predicate adapted = resolver.resolve(schemaId3);

        // In schema v3, column "a" (colId=1) is at index 1 (second position)
        assertThat(adapted).isNotNull();
        assertThat(adapted).isInstanceOf(LeafPredicate.class);
        LeafPredicate leaf = (LeafPredicate) adapted;
        assertThat(leaf.index()).isEqualTo(1);
        assertThat(leaf.fieldName()).isEqualTo("a");
    }

    @Test
    void testBatchSchemaNotFoundReturnsNull() {
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID_1, SCHEMA_V1);
        // Schema ID 99 is not registered in the getter

        Predicate predicate = new PredicateBuilder(ROW_TYPE_V1).equal(0, 42);
        PredicateSchemaResolver resolver =
                new PredicateSchemaResolver(predicate, SCHEMA_ID_1, schemaGetter);

        // Should return null because batch schema 99 doesn't exist
        assertThat(resolver.resolve(99)).isNull();
    }

    @Test
    void testAdaptationForDroppedColumnReturnsNull() {
        // Schema v1: (a INT colId=1, b STRING colId=2)
        // Schema v4: (c STRING colId=3) — column "a" was dropped
        Schema schemaV4 =
                Schema.newBuilder()
                        .fromColumns(
                                Arrays.asList(new Schema.Column("c", DataTypes.STRING(), null, 3)))
                        .build();
        int schemaId4 = 4;

        // Predicate on column "a" at index 0 in schema v1
        Predicate predicate = new PredicateBuilder(ROW_TYPE_V1).equal(0, 42);

        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID_1, SCHEMA_V1);
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(schemaV4, schemaId4));

        PredicateSchemaResolver resolver =
                new PredicateSchemaResolver(predicate, SCHEMA_ID_1, schemaGetter);

        // Column "a" doesn't exist in schema v4, so adaptation should return null
        // (transformFieldMapping returns Optional.empty for unmapped fields)
        assertThat(resolver.resolve(schemaId4)).isNull();
    }
}
