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

package com.alibaba.fluss.metrics.prometheus;

import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.util.TestMetricGroup;

import java.util.LinkedHashMap;
import java.util.Map;

class TestUtils {

    public static MetricGroup createTestMetricGroup(
            String logicalScope, Map<String, String> variables) {
        return TestMetricGroup.newBuilder()
                .setLogicalScopeFunction(
                        (characterFilter, character) ->
                                characterFilter.filterCharacters(logicalScope))
                .setVariables(variables)
                .build();
    }

    public static Map<String, String> toMap(String[] labels, String[] values) {
        // when querying metrics the order of labels is important; use insertion order for
        // simplicity
        final Map<String, String> variables = new LinkedHashMap<>();
        for (int i = 0; i < labels.length; i++) {
            variables.put(labels[i], values[i]);
        }

        return variables;
    }
}
