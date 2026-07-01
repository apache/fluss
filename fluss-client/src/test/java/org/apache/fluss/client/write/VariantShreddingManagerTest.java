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

package org.apache.fluss.client.write;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.variant.FieldStatistics;
import org.apache.fluss.types.variant.ShreddingSchema;
import org.apache.fluss.types.variant.ShreddingSchemaInferrer;
import org.apache.fluss.types.variant.Variant;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VariantShreddingManager}. */
class VariantShreddingManagerTest {

    @Test
    void testInferenceCompletesOnceWhenNoFieldsEligible() {
        CountingInferrer inferrer = new CountingInferrer();
        inferrer.setMinSampleSize(3);
        VariantShreddingManager manager = newManager(inferrer);

        for (int i = 0; i < 5; i++) {
            manager.collectRow(row(Variant.fromJson("{\"nested\":{\"id\":1}}")));
        }

        assertThat(manager.getShreddingSchemas()).isEmpty();
        assertThat(inferrer.getInferCalls()).isEqualTo(1);
    }

    @Test
    void testInferenceCompletesOnceWhenFieldsAreEligible() {
        CountingInferrer inferrer = new CountingInferrer();
        inferrer.setMinSampleSize(3);
        VariantShreddingManager manager = newManager(inferrer);

        for (int i = 0; i < 5; i++) {
            manager.collectRow(row(Variant.fromJson("{\"name\":\"user\"}")));
        }

        assertThat(manager.getShreddingSchemas()).containsOnlyKeys("data");
        assertThat(manager.getShreddingSchemas().get("data").getFields())
                .extracting(field -> field.getFieldPath())
                .containsExactly("name");
        assertThat(inferrer.getInferCalls()).isEqualTo(1);
    }

    private static VariantShreddingManager newManager(ShreddingSchemaInferrer inferrer) {
        return new VariantShreddingManager(
                TablePath.of("test_db", "test_table"),
                new int[] {0},
                new String[] {"data"},
                inferrer);
    }

    private static final class CountingInferrer extends ShreddingSchemaInferrer {
        private int inferCalls;

        @Override
        public ShreddingSchema infer(
                String variantColumnName, Map<String, FieldStatistics> stats, long totalRecords) {
            inferCalls++;
            return super.infer(variantColumnName, stats, totalRecords);
        }

        int getInferCalls() {
            return inferCalls;
        }
    }
}
