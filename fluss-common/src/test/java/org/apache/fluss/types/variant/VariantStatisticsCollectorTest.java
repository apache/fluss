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

package org.apache.fluss.types.variant;

import org.apache.fluss.types.DataTypeRoot;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VariantStatisticsCollector}. */
class VariantStatisticsCollectorTest {

    // --------------------------------------------------------------------------------------------
    // Single record collection
    // --------------------------------------------------------------------------------------------

    @Test
    void testCollectSingleObjectRecord() {
        VariantStatisticsCollector collector = new VariantStatisticsCollector();

        Variant v = Variant.fromJson("{\"name\":\"Alice\",\"age\":30,\"active\":true}");
        collector.collect(v);

        assertThat(collector.getTotalRecords()).isEqualTo(1);
        Map<String, FieldStatistics> stats = collector.getStatistics();
        assertThat(stats).containsKey("name");
        assertThat(stats).containsKey("age");
        assertThat(stats).containsKey("active");

        assertThat(stats.get("name").getPresenceCount()).isEqualTo(1);
        assertThat(stats.get("active").dominantType()).isEqualTo(DataTypeRoot.BOOLEAN);
    }

    @Test
    void testCollectNullVariant() {
        VariantStatisticsCollector collector = new VariantStatisticsCollector();
        collector.collect(null);

        assertThat(collector.getTotalRecords()).isEqualTo(1);
        assertThat(collector.getStatistics()).isEmpty();
    }

    @Test
    void testCollectNullValueVariant() {
        VariantStatisticsCollector collector = new VariantStatisticsCollector();
        collector.collect(Variant.ofNull());

        assertThat(collector.getTotalRecords()).isEqualTo(1);
        assertThat(collector.getStatistics()).isEmpty();
    }

    @Test
    void testCollectNonObjectVariant() {
        VariantStatisticsCollector collector = new VariantStatisticsCollector();
        collector.collect(Variant.ofInt(42));

        assertThat(collector.getTotalRecords()).isEqualTo(1);
        assertThat(collector.getStatistics()).isEmpty();
    }

    // --------------------------------------------------------------------------------------------
    // Batch collection
    // --------------------------------------------------------------------------------------------

    @Test
    void testCollectBatch() {
        VariantStatisticsCollector collector = new VariantStatisticsCollector();

        collector.collectBatch(
                Arrays.asList(
                        Variant.fromJson("{\"name\":\"Alice\",\"age\":30}"),
                        Variant.fromJson("{\"name\":\"Bob\",\"age\":25}"),
                        Variant.fromJson("{\"name\":\"Charlie\"}")));

        assertThat(collector.getTotalRecords()).isEqualTo(3);
        Map<String, FieldStatistics> stats = collector.getStatistics();

        assertThat(stats.get("name").getPresenceCount()).isEqualTo(3);
        assertThat(stats.get("name").presenceRatio(3)).isEqualTo(1.0f);
        assertThat(stats.get("age").getPresenceCount()).isEqualTo(2);
    }

    // --------------------------------------------------------------------------------------------
    // Type inference
    // --------------------------------------------------------------------------------------------

    @Test
    void testTypeInferenceFromJsonValues() {
        VariantStatisticsCollector collector = new VariantStatisticsCollector();

        collector.collect(Variant.fromJson("{\"val\":42}"));
        collector.collect(Variant.fromJson("{\"val\":100}"));

        Map<String, FieldStatistics> stats = collector.getStatistics();
        FieldStatistics valStats = stats.get("val");
        assertThat(valStats.getPresenceCount()).isEqualTo(2);
        // JSON parser encodes small integers as INT8
        assertThat(valStats.typeConsistency()).isEqualTo(1.0f);
    }

    @Test
    void testMixedTypeTracking() {
        VariantStatisticsCollector collector = new VariantStatisticsCollector();

        // "value" field has different types across records
        collector.collect(Variant.fromJson("{\"value\":42}"));
        collector.collect(Variant.fromJson("{\"value\":\"hello\"}"));
        collector.collect(Variant.fromJson("{\"value\":true}"));

        Map<String, FieldStatistics> stats = collector.getStatistics();
        FieldStatistics valueStats = stats.get("value");
        assertThat(valueStats.getPresenceCount()).isEqualTo(3);
        // Three different types => consistency should be 1/3
        assertThat(valueStats.typeConsistency()).isLessThan(0.5f);
    }

    // --------------------------------------------------------------------------------------------
    // Merge
    // --------------------------------------------------------------------------------------------

    @Test
    void testMerge() {
        VariantStatisticsCollector collector1 = new VariantStatisticsCollector();
        collector1.collect(Variant.fromJson("{\"name\":\"Alice\",\"age\":30}"));
        collector1.collect(Variant.fromJson("{\"name\":\"Bob\",\"age\":25}"));

        VariantStatisticsCollector collector2 = new VariantStatisticsCollector();
        collector2.collect(Variant.fromJson("{\"name\":\"Charlie\",\"age\":35}"));
        collector2.collect(Variant.fromJson("{\"name\":\"Diana\"}"));

        collector1.merge(collector2);

        assertThat(collector1.getTotalRecords()).isEqualTo(4);
        Map<String, FieldStatistics> stats = collector1.getStatistics();
        assertThat(stats.get("name").getPresenceCount()).isEqualTo(4);
        assertThat(stats.get("age").getPresenceCount()).isEqualTo(3);
    }

    @Test
    void testMergeEmptyCollectors() {
        VariantStatisticsCollector collector1 = new VariantStatisticsCollector();
        VariantStatisticsCollector collector2 = new VariantStatisticsCollector();

        collector1.merge(collector2);
        assertThat(collector1.getTotalRecords()).isZero();
        assertThat(collector1.getStatistics()).isEmpty();
    }

    @Test
    void testMergeWithDisjointFields() {
        VariantStatisticsCollector collector1 = new VariantStatisticsCollector();
        collector1.collect(Variant.fromJson("{\"field_a\":1}"));

        VariantStatisticsCollector collector2 = new VariantStatisticsCollector();
        collector2.collect(Variant.fromJson("{\"field_b\":2}"));

        collector1.merge(collector2);

        assertThat(collector1.getTotalRecords()).isEqualTo(2);
        assertThat(collector1.getStatistics()).containsKey("field_a");
        assertThat(collector1.getStatistics()).containsKey("field_b");
    }

    // --------------------------------------------------------------------------------------------
    // Reset
    // --------------------------------------------------------------------------------------------

    @Test
    void testReset() {
        VariantStatisticsCollector collector = new VariantStatisticsCollector();
        collector.collect(Variant.fromJson("{\"name\":\"Alice\"}"));
        assertThat(collector.getTotalRecords()).isEqualTo(1);

        collector.reset();
        assertThat(collector.getTotalRecords()).isZero();
        assertThat(collector.getStatistics()).isEmpty();
    }

    // --------------------------------------------------------------------------------------------
    // End-to-end with ShreddingSchemaInferrer
    // --------------------------------------------------------------------------------------------

    @Test
    void testEndToEndWithInferrer() {
        VariantStatisticsCollector collector = new VariantStatisticsCollector();

        // Simulate collecting 1500 records
        for (int i = 0; i < 1500; i++) {
            if (i % 3 == 0) {
                // Every 3rd record has "status" field
                collector.collect(
                        Variant.fromJson(
                                "{\"name\":\"user"
                                        + i
                                        + "\",\"age\":"
                                        + (20 + i % 50)
                                        + ",\"status\":\"active\"}"));
            } else {
                collector.collect(
                        Variant.fromJson(
                                "{\"name\":\"user" + i + "\",\"age\":" + (20 + i % 50) + "}"));
            }
        }

        assertThat(collector.getTotalRecords()).isEqualTo(1500);

        ShreddingSchemaInferrer inferrer = new ShreddingSchemaInferrer();
        ShreddingSchema schema =
                inferrer.infer("data", collector.getStatistics(), collector.getTotalRecords());

        // "name" and "age" appear in 100% of records => should be shredded
        // "status" appears in ~33% of records => below 50% threshold => not shredded
        boolean hasName = false;
        boolean hasAge = false;
        boolean hasStatus = false;
        for (ShreddedField field : schema.getFields()) {
            if (field.getFieldPath().equals("name")) {
                hasName = true;
            }
            if (field.getFieldPath().equals("age")) {
                hasAge = true;
            }
            if (field.getFieldPath().equals("status")) {
                hasStatus = true;
            }
        }
        assertThat(hasName).isTrue();
        assertThat(hasAge).isTrue();
        assertThat(hasStatus).isFalse();
    }
}
