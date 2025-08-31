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

package org.apache.fluss.plugin;

import org.apache.fluss.config.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.config.ConfigOptions.PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS;
import static org.apache.fluss.config.ConfigOptions.PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PluginConfig} . */
class PluginConfigTest {

    @Test
    void testFromConfig() {
        // only set parent-first-patterns.default
        Configuration config = new Configuration();
        config.set(
                PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS,
                Arrays.asList("org.apache.fluss.", "java."));
        PluginConfig pluginConfig = PluginConfig.fromConfiguration(config);
        assertThat(pluginConfig.getAlwaysParentFirstPatterns())
                .isEqualTo(new String[] {"org.apache.fluss.", "java."});
        // also set parent-first-patterns.additional
        config.set(
                PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.hadoop."));
        pluginConfig = PluginConfig.fromConfiguration(config);
        assertThat(pluginConfig.getAlwaysParentFirstPatterns())
                .isEqualTo(new String[] {"org.apache.fluss.", "java.", "org.apache.hadoop."});
    }
}
