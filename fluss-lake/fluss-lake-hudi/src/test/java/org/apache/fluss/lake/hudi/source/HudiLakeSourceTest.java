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

package org.apache.fluss.lake.hudi.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HudiLakeSource}. */
class HudiLakeSourceTest {

    @Test
    void testCreatePlannerReturnsHudiSplitPlanner() throws Exception {
        HudiLakeSource source =
                new HudiLakeSource(new Configuration(), TablePath.of("db1", "table1"));

        assertThat(source.createPlanner(() -> 1L)).isInstanceOf(HudiSplitPlanner.class);
    }

    @Test
    void testGetSplitSerializerReturnsHudiSplitSerializer() {
        HudiLakeSource source =
                new HudiLakeSource(new Configuration(), TablePath.of("db1", "table1"));

        assertThat(source.getSplitSerializer()).isInstanceOf(HudiSplitSerializer.class);
    }

    @Test
    void testWithEmptyFiltersDoesNotResolveHudiSchema() {
        HudiLakeSource source =
                new HudiLakeSource(new Configuration(), TablePath.of("db1", "table1"));

        LakeSource.FilterPushDownResult result = source.withFilters(Collections.emptyList());

        assertThat(result.acceptedPredicates()).isEmpty();
        assertThat(result.remainingPredicates()).isEmpty();
    }

    @Test
    void testWithLimitIsExplicitlyUnsupported() {
        HudiLakeSource source =
                new HudiLakeSource(new Configuration(), TablePath.of("db1", "table1"));

        assertThatThrownBy(() -> source.withLimit(1))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("limit");
    }
}
