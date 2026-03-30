/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.config;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link StatisticsColumnsConfig}. */
class StatisticsColumnsConfigTest {

    @Test
    void testDisabledMode() {
        StatisticsColumnsConfig config = StatisticsColumnsConfig.disabled();
        assertThat(config.getMode()).isEqualTo(StatisticsColumnsConfig.Mode.DISABLED);
        assertThat(config.getColumns()).isEmpty();
        assertThat(config.isEnabled()).isFalse();
    }

    @Test
    void testAllMode() {
        StatisticsColumnsConfig config = StatisticsColumnsConfig.all();
        assertThat(config.getMode()).isEqualTo(StatisticsColumnsConfig.Mode.ALL);
        assertThat(config.getColumns()).isEmpty();
        assertThat(config.isEnabled()).isTrue();
    }

    @Test
    void testSpecifiedMode() {
        List<String> columns = Arrays.asList("col1", "col2", "col3");
        StatisticsColumnsConfig config = StatisticsColumnsConfig.of(columns);
        assertThat(config.getMode()).isEqualTo(StatisticsColumnsConfig.Mode.SPECIFIED);
        assertThat(config.getColumns()).containsExactly("col1", "col2", "col3");
        assertThat(config.isEnabled()).isTrue();
    }

    @Test
    void testSpecifiedModeWithEmptyList() {
        StatisticsColumnsConfig config = StatisticsColumnsConfig.of(Collections.emptyList());
        assertThat(config.getMode()).isEqualTo(StatisticsColumnsConfig.Mode.SPECIFIED);
        assertThat(config.getColumns()).isEmpty();
        assertThat(config.isEnabled()).isTrue();
    }

    @Test
    void testSpecifiedModeColumnsAreUnmodifiable() {
        List<String> columns = Arrays.asList("col1", "col2");
        StatisticsColumnsConfig config = StatisticsColumnsConfig.of(columns);
        assertThatThrownBy(() -> config.getColumns().add("col3"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testOfWithNullThrows() {
        assertThatThrownBy(() -> StatisticsColumnsConfig.of(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("columns must not be null");
    }

    @Test
    void testDisabledSingleton() {
        StatisticsColumnsConfig config1 = StatisticsColumnsConfig.disabled();
        StatisticsColumnsConfig config2 = StatisticsColumnsConfig.disabled();
        assertThat(config1).isSameAs(config2);
    }

    @Test
    void testAllSingleton() {
        StatisticsColumnsConfig config1 = StatisticsColumnsConfig.all();
        StatisticsColumnsConfig config2 = StatisticsColumnsConfig.all();
        assertThat(config1).isSameAs(config2);
    }

    @Test
    void testEqualsAndHashCode() {
        // same mode and columns
        StatisticsColumnsConfig specified1 =
                StatisticsColumnsConfig.of(Arrays.asList("col1", "col2"));
        StatisticsColumnsConfig specified2 =
                StatisticsColumnsConfig.of(Arrays.asList("col1", "col2"));
        assertThat(specified1).isEqualTo(specified2);
        assertThat(specified1.hashCode()).isEqualTo(specified2.hashCode());

        // same instance
        assertThat(specified1).isEqualTo(specified1);

        // null and different type
        assertThat(specified1).isNotEqualTo(null);
        assertThat(specified1).isNotEqualTo("not a config");

        // different columns
        StatisticsColumnsConfig specified3 =
                StatisticsColumnsConfig.of(Arrays.asList("col1", "col3"));
        assertThat(specified1).isNotEqualTo(specified3);

        // different modes
        assertThat(StatisticsColumnsConfig.disabled()).isNotEqualTo(StatisticsColumnsConfig.all());
        assertThat(StatisticsColumnsConfig.disabled()).isNotEqualTo(specified1);
        assertThat(StatisticsColumnsConfig.all()).isNotEqualTo(specified1);

        // disabled equals disabled
        assertThat(StatisticsColumnsConfig.disabled())
                .isEqualTo(StatisticsColumnsConfig.disabled());
        assertThat(StatisticsColumnsConfig.disabled().hashCode())
                .isEqualTo(StatisticsColumnsConfig.disabled().hashCode());

        // all equals all
        assertThat(StatisticsColumnsConfig.all()).isEqualTo(StatisticsColumnsConfig.all());
        assertThat(StatisticsColumnsConfig.all().hashCode())
                .isEqualTo(StatisticsColumnsConfig.all().hashCode());
    }

    @Test
    void testToString() {
        assertThat(StatisticsColumnsConfig.disabled().toString())
                .isEqualTo("StatisticsColumnsConfig{DISABLED}");

        assertThat(StatisticsColumnsConfig.all().toString())
                .isEqualTo("StatisticsColumnsConfig{ALL}");

        StatisticsColumnsConfig specified =
                StatisticsColumnsConfig.of(Arrays.asList("col1", "col2"));
        assertThat(specified.toString())
                .isEqualTo("StatisticsColumnsConfig{SPECIFIED: [col1, col2]}");
    }

    @Test
    void testSingleColumnSpecified() {
        StatisticsColumnsConfig config =
                StatisticsColumnsConfig.of(Collections.singletonList("single_col"));
        assertThat(config.getMode()).isEqualTo(StatisticsColumnsConfig.Mode.SPECIFIED);
        assertThat(config.getColumns()).containsExactly("single_col");
        assertThat(config.isEnabled()).isTrue();
        assertThat(config.toString()).isEqualTo("StatisticsColumnsConfig{SPECIFIED: [single_col]}");
    }
}
