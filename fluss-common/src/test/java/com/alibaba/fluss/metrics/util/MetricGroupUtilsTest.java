/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.metrics.util;

import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.utils.MetricGroupUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MetricGroupUtils}. */
class MetricGroupUtilsTest {

    @Test
    void testMakeScope() {
        MetricGroup metricGroup =
                TestMetricGroup.newBuilder().setScopeComponents(new String[0]).build();

        String[] scope = MetricGroupUtils.makeScope(metricGroup, "a", "b", "c");

        assertThat(scope).isEqualTo(new String[] {"a", "b", "c"});

        metricGroup =
                TestMetricGroup.newBuilder().setScopeComponents(new String[] {"a", "b"}).build();

        scope = MetricGroupUtils.makeScope(metricGroup, "c", "d");
        assertThat(scope).isEqualTo(new String[] {"a", "b", "c", "d"});
    }
}
