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

package org.apache.fluss.flink.sink;

import org.apache.fluss.flink.sink.undo.RecoveryAction;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.metadata.AggFunctions.MAX;
import static org.apache.fluss.metadata.AggFunctions.SUM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AggregationRecoveryDecider}. */
class AggregationRecoveryDeciderTest {

    private static final Schema MIXED_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .column("max_value", DataTypes.BIGINT(), MAX())
                    .column("sum_value", DataTypes.BIGINT(), SUM())
                    .column("implicit_last", DataTypes.STRING())
                    .primaryKey("id")
                    .build();
    private static final Schema SAFE_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .column("max_value", DataTypes.BIGINT(), MAX())
                    .column("implicit_last", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @Test
    void testFullAndPartialTargets() {
        assertThat(decide(SAFE_SCHEMA, null, DeleteBehavior.ALLOW, false, true))
                .isEqualTo(RecoveryAction.NO_OP);
        assertThat(decide(MIXED_SCHEMA, null, DeleteBehavior.ALLOW, false, true))
                .isEqualTo(RecoveryAction.UNDO);
        assertThat(decide(MIXED_SCHEMA, new int[] {0, 1, 3}, DeleteBehavior.ALLOW, false, true))
                .isEqualTo(RecoveryAction.NO_OP);
        assertThat(decide(MIXED_SCHEMA, new int[] {0, 2}, DeleteBehavior.ALLOW, false, true))
                .isEqualTo(RecoveryAction.UNDO);
    }

    @Test
    void testPrimaryKeyAndImplicitLastValueAreSafe() {
        assertThat(decide(SAFE_SCHEMA, new int[] {0}, DeleteBehavior.ALLOW, false, true))
                .isEqualTo(RecoveryAction.NO_OP);
        assertThat(decide(SAFE_SCHEMA, new int[] {0, 2}, DeleteBehavior.ALLOW, false, true))
                .isEqualTo(RecoveryAction.NO_OP);
    }

    @Test
    void testInputNotKnownUpsertOnlyUsesConservativeDeletePolicy() {
        assertThat(decide(SAFE_SCHEMA, null, DeleteBehavior.ALLOW, false, false))
                .isEqualTo(RecoveryAction.UNDO);
        assertThat(decide(SAFE_SCHEMA, null, DeleteBehavior.IGNORE, false, false))
                .isEqualTo(RecoveryAction.NO_OP);
        assertThat(decide(SAFE_SCHEMA, null, DeleteBehavior.DISABLE, false, false))
                .isEqualTo(RecoveryAction.NO_OP);
        assertThat(decide(SAFE_SCHEMA, null, DeleteBehavior.ALLOW, true, false))
                .isEqualTo(RecoveryAction.NO_OP);
    }

    @Test
    void testInvalidTargetsStillFailFast() {
        assertThatThrownBy(
                        () ->
                                decide(
                                        SAFE_SCHEMA,
                                        new int[] {-1},
                                        DeleteBehavior.ALLOW,
                                        false,
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid target column index -1");
        assertThatThrownBy(
                        () ->
                                decide(
                                        SAFE_SCHEMA,
                                        new int[] {3},
                                        DeleteBehavior.ALLOW,
                                        false,
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid target column index 3");
    }

    private static RecoveryAction decide(
            Schema schema,
            int[] targets,
            DeleteBehavior deleteBehavior,
            boolean ignoreDelete,
            boolean inputKnownUpsertOnly) {
        return AggregationRecoveryDecider.decide(
                schema, targets, deleteBehavior, ignoreDelete, inputKnownUpsertOnly);
    }
}
