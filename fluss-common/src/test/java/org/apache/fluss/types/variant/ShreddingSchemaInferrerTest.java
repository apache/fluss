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
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ShreddingSchemaInferrer}. */
class ShreddingSchemaInferrerTest {

    // --------------------------------------------------------------------------------------------
    // Basic inference
    // --------------------------------------------------------------------------------------------

    @Test
    void testInferWithSufficientData() {
        // 2000 records, "age" always INT, "name" always STRING
        Map<String, FieldStatistics> stats = new HashMap<>();
        FieldStatistics ageStats = new FieldStatistics("age");
        for (int i = 0; i < 2000; i++) {
            ageStats.record(DataTypeRoot.INTEGER);
        }
        stats.put("age", ageStats);

        FieldStatistics nameStats = new FieldStatistics("name");
        for (int i = 0; i < 1800; i++) {
            nameStats.record(DataTypeRoot.STRING);
        }
        stats.put("name", nameStats);

        ShreddingSchemaInferrer inferrer = new ShreddingSchemaInferrer();
        ShreddingSchema schema = inferrer.infer("event", stats, 2000);

        assertThat(schema.getVariantColumnName()).isEqualTo("event");
        assertThat(schema.getFields()).hasSize(2);

        // "age" has presence 1.0, consistency 1.0 => score 1.0
        // "name" has presence 0.9, consistency 1.0 => score 0.9
        // Both should qualify
        List<String> fieldPaths = new ArrayList<>();
        for (ShreddedField f : schema.getFields()) {
            fieldPaths.add(f.getFieldPath());
        }
        assertThat(fieldPaths).contains("age", "name");
    }

    @Test
    void testInferInsufficientSampleSize() {
        Map<String, FieldStatistics> stats = new HashMap<>();
        FieldStatistics ageStats = new FieldStatistics("age");
        for (int i = 0; i < 500; i++) {
            ageStats.record(DataTypeRoot.INTEGER);
        }
        stats.put("age", ageStats);

        ShreddingSchemaInferrer inferrer = new ShreddingSchemaInferrer();
        // Only 500 records, default min is 1000
        ShreddingSchema schema = inferrer.infer("event", stats, 500);

        assertThat(schema.getFields()).isEmpty();
    }

    @Test
    void testInferCustomMinSampleSize() {
        Map<String, FieldStatistics> stats = new HashMap<>();
        FieldStatistics ageStats = new FieldStatistics("age");
        for (int i = 0; i < 100; i++) {
            ageStats.record(DataTypeRoot.INTEGER);
        }
        stats.put("age", ageStats);

        ShreddingSchemaInferrer inferrer = new ShreddingSchemaInferrer().setMinSampleSize(50);
        ShreddingSchema schema = inferrer.infer("event", stats, 100);

        assertThat(schema.getFields()).hasSize(1);
        assertThat(schema.getFields().get(0).getFieldPath()).isEqualTo("age");
    }

    // --------------------------------------------------------------------------------------------
    // Threshold filtering
    // --------------------------------------------------------------------------------------------

    @Test
    void testInferPresenceThresholdFiltering() {
        Map<String, FieldStatistics> stats = new HashMap<>();

        // "age" appears in 30% of records => below default 50% threshold
        FieldStatistics ageStats = new FieldStatistics("age");
        for (int i = 0; i < 300; i++) {
            ageStats.record(DataTypeRoot.INTEGER);
        }
        stats.put("age", ageStats);

        // "name" appears in 80% of records => above threshold
        FieldStatistics nameStats = new FieldStatistics("name");
        for (int i = 0; i < 800; i++) {
            nameStats.record(DataTypeRoot.STRING);
        }
        stats.put("name", nameStats);

        ShreddingSchemaInferrer inferrer = new ShreddingSchemaInferrer().setMinSampleSize(100);
        ShreddingSchema schema = inferrer.infer("event", stats, 1000);

        assertThat(schema.getFields()).hasSize(1);
        assertThat(schema.getFields().get(0).getFieldPath()).isEqualTo("name");
    }

    @Test
    void testInferTypeConsistencyFiltering() {
        Map<String, FieldStatistics> stats = new HashMap<>();

        // "value" has 50% INT and 50% STRING => consistency = 0.5 < 0.9 threshold
        FieldStatistics valueStats = new FieldStatistics("value");
        for (int i = 0; i < 500; i++) {
            valueStats.record(DataTypeRoot.INTEGER);
        }
        for (int i = 0; i < 500; i++) {
            valueStats.record(DataTypeRoot.STRING);
        }
        stats.put("value", valueStats);

        ShreddingSchemaInferrer inferrer = new ShreddingSchemaInferrer().setMinSampleSize(100);
        ShreddingSchema schema = inferrer.infer("event", stats, 1000);

        assertThat(schema.getFields()).isEmpty();
    }

    // --------------------------------------------------------------------------------------------
    // Max fields limiting
    // --------------------------------------------------------------------------------------------

    @Test
    void testInferMaxFieldsLimiting() {
        Map<String, FieldStatistics> stats = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            FieldStatistics fs = new FieldStatistics("field" + i);
            for (int j = 0; j < 1000; j++) {
                fs.record(DataTypeRoot.INTEGER);
            }
            stats.put("field" + i, fs);
        }

        ShreddingSchemaInferrer inferrer =
                new ShreddingSchemaInferrer().setMinSampleSize(100).setMaxShreddedFields(3);
        ShreddingSchema schema = inferrer.infer("event", stats, 1000);

        assertThat(schema.getFields()).hasSize(3);
    }

    // --------------------------------------------------------------------------------------------
    // Type mapping
    // --------------------------------------------------------------------------------------------

    @Test
    void testInferTypeMapping() {
        Map<String, FieldStatistics> stats = new HashMap<>();

        FieldStatistics boolStats = new FieldStatistics("flag");
        for (int i = 0; i < 1000; i++) {
            boolStats.record(DataTypeRoot.BOOLEAN);
        }
        stats.put("flag", boolStats);

        FieldStatistics doubleStats = new FieldStatistics("ratio");
        for (int i = 0; i < 1000; i++) {
            doubleStats.record(DataTypeRoot.DOUBLE);
        }
        stats.put("ratio", doubleStats);

        ShreddingSchemaInferrer inferrer = new ShreddingSchemaInferrer().setMinSampleSize(100);
        ShreddingSchema schema = inferrer.infer("event", stats, 1000);

        assertThat(schema.getFields()).hasSize(2);
        for (ShreddedField field : schema.getFields()) {
            if (field.getFieldPath().equals("flag")) {
                assertThat(field.getShreddedType()).isEqualTo(DataTypes.BOOLEAN());
            } else if (field.getFieldPath().equals("ratio")) {
                assertThat(field.getShreddedType()).isEqualTo(DataTypes.DOUBLE());
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Incremental update
    // --------------------------------------------------------------------------------------------

    @Test
    void testUpdateAddNewFields() {
        // Start with schema having "age" shredded
        ShreddingSchema current =
                new ShreddingSchema(
                        "event", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 5)));

        // New stats show "age" still qualifies + "name" now qualifies
        Map<String, FieldStatistics> newStats = new HashMap<>();
        FieldStatistics ageStats = new FieldStatistics("age");
        for (int i = 0; i < 1000; i++) {
            ageStats.record(DataTypeRoot.INTEGER);
        }
        newStats.put("age", ageStats);

        FieldStatistics nameStats = new FieldStatistics("name");
        for (int i = 0; i < 800; i++) {
            nameStats.record(DataTypeRoot.STRING);
        }
        newStats.put("name", nameStats);

        ShreddingSchemaInferrer inferrer = new ShreddingSchemaInferrer().setMinSampleSize(100);
        Optional<ShreddingSchema> updated = inferrer.update(current, newStats, 1000);

        assertThat(updated).isPresent();
        assertThat(updated.get().getFields()).hasSize(2);
    }

    @Test
    void testUpdateNoChanges() {
        ShreddingSchema current =
                new ShreddingSchema(
                        "event", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 5)));

        // "age" still qualifies, no new fields
        Map<String, FieldStatistics> newStats = new HashMap<>();
        FieldStatistics ageStats = new FieldStatistics("age");
        for (int i = 0; i < 1000; i++) {
            ageStats.record(DataTypeRoot.INTEGER);
        }
        newStats.put("age", ageStats);

        ShreddingSchemaInferrer inferrer = new ShreddingSchemaInferrer().setMinSampleSize(100);
        Optional<ShreddingSchema> updated = inferrer.update(current, newStats, 1000);

        assertThat(updated).isEmpty();
    }

    @Test
    void testUpdateDemoteField() {
        ShreddingSchema current =
                new ShreddingSchema(
                        "event",
                        Arrays.asList(
                                new ShreddedField("age", DataTypes.INT(), 5),
                                new ShreddedField("name", DataTypes.STRING(), 6)));

        // "name" no longer in stats => will be demoted
        Map<String, FieldStatistics> newStats = new HashMap<>();
        FieldStatistics ageStats = new FieldStatistics("age");
        for (int i = 0; i < 1000; i++) {
            ageStats.record(DataTypeRoot.INTEGER);
        }
        newStats.put("age", ageStats);

        ShreddingSchemaInferrer inferrer = new ShreddingSchemaInferrer().setMinSampleSize(100);
        Optional<ShreddingSchema> updated = inferrer.update(current, newStats, 1000);

        assertThat(updated).isPresent();
        assertThat(updated.get().getFields()).hasSize(1);
        assertThat(updated.get().getFields().get(0).getFieldPath()).isEqualTo("age");
    }

    @Test
    void testUpdateInsufficientSamples() {
        ShreddingSchema current =
                new ShreddingSchema(
                        "event", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 5)));

        Map<String, FieldStatistics> newStats = new HashMap<>();
        ShreddingSchemaInferrer inferrer = new ShreddingSchemaInferrer();
        Optional<ShreddingSchema> updated = inferrer.update(current, newStats, 500);

        assertThat(updated).isEmpty();
    }

    // --------------------------------------------------------------------------------------------
    // Getters/Setters
    // --------------------------------------------------------------------------------------------

    @Test
    void testGettersSetters() {
        ShreddingSchemaInferrer inferrer =
                new ShreddingSchemaInferrer()
                        .setPresenceThreshold(0.7f)
                        .setTypeConsistencyThreshold(0.95f)
                        .setMaxShreddedFields(50)
                        .setMinSampleSize(500);

        assertThat(inferrer.getPresenceThreshold()).isEqualTo(0.7f);
        assertThat(inferrer.getTypeConsistencyThreshold()).isEqualTo(0.95f);
        assertThat(inferrer.getMaxShreddedFields()).isEqualTo(50);
        assertThat(inferrer.getMinSampleSize()).isEqualTo(500);
    }
}
