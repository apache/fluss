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

package com.alibaba.fluss.config;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link com.alibaba.fluss.config.ConfigOption} and {@link
 * com.alibaba.fluss.config.ConfigBuilder}.
 */
public class ConfigOptionTest {

    @Test
    void testDescription() {
        final ConfigOption<Integer> option =
                ConfigBuilder.key("key").intType().defaultValue(0).withDescription("hello key");

        assertThat(option.description()).isEqualTo("hello key");

        final ConfigOption<Integer> option2 = ConfigBuilder.key("key2").intType().defaultValue(0);
        assertThat(option2.description()).isEqualTo("");
    }

    @Test
    void testDefaultValue() {
        final ConfigOption<Integer> option = ConfigBuilder.key("key").intType().noDefaultValue();
        assertThat(option.hasDefaultValue()).isFalse();

        final ConfigOption<Integer> option2 = ConfigBuilder.key("key2").intType().defaultValue(1);
        assertThat(option2.hasDefaultValue()).isTrue();
        assertThat(option2.defaultValue()).isEqualTo(1);
    }

    @Test
    void testDeprecationFlagForDeprecatedKeys() {
        final ConfigOption<Integer> optionWithDeprecatedKeys =
                ConfigBuilder.key("key")
                        .intType()
                        .defaultValue(0)
                        .withDeprecatedKeys("deprecated1", "deprecated2");

        assertThat(optionWithDeprecatedKeys.hasFallbackKeys()).isTrue();
        for (final FallbackKey fallbackKey : optionWithDeprecatedKeys.fallbackKeys()) {
            assertThat(fallbackKey.isDeprecated()).isTrue();
        }
    }

    @Test
    void testDeprecationFlagForFallbackKeys() {
        final ConfigOption<Integer> optionWithFallbackKeys =
                ConfigBuilder.key("key")
                        .intType()
                        .defaultValue(0)
                        .withFallbackKeys("fallback1", "fallback2");

        assertThat(optionWithFallbackKeys.hasFallbackKeys()).isTrue();
        assertThat(optionWithFallbackKeys.fallbackKeys()).allMatch(f -> !f.isDeprecated());
    }

    @Test
    void testDeprecationFlagForMixedAlternativeKeys() {
        final ConfigOption<Integer> optionWithMixedKeys =
                ConfigBuilder.key("key")
                        .intType()
                        .defaultValue(0)
                        .withDeprecatedKeys("deprecated1", "deprecated2")
                        .withFallbackKeys("fallback1", "fallback2");

        final List<String> fallbackKeys = new ArrayList<>(2);
        final List<String> deprecatedKeys = new ArrayList<>(2);
        for (final FallbackKey alternativeKey : optionWithMixedKeys.fallbackKeys()) {
            if (alternativeKey.isDeprecated()) {
                deprecatedKeys.add(alternativeKey.getKey());
            } else {
                fallbackKeys.add(alternativeKey.getKey());
            }
        }

        assertThat(fallbackKeys.size()).isEqualTo(2);
        assertThat(deprecatedKeys.size()).isEqualTo(2);

        assertThat(Stream.of("fallback1", "fallback2").sorted().collect(Collectors.toList()))
                .isEqualTo(fallbackKeys.stream().sorted().collect(Collectors.toList()));
        assertThat(Stream.of("deprecated1", "deprecated2").sorted().collect(Collectors.toList()))
                .isEqualTo(deprecatedKeys.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void testOptionComparison() {
        ConfigOption<Integer> opt1 =
                ConfigBuilder.key("key")
                        .intType()
                        .defaultValue(0)
                        .withDeprecatedKeys("deprecated1", "deprecated2")
                        .withFallbackKeys("fallback1", "fallback2");

        ConfigOption<Integer> opt2 =
                ConfigBuilder.key("key")
                        .intType()
                        .defaultValue(0)
                        .withDeprecatedKeys("deprecated1", "deprecated2")
                        .withFallbackKeys("fallback1", "fallback2");

        assertThat(opt1).isEqualTo(opt2);
        assertThat(opt1.hashCode()).isEqualTo(opt2.hashCode());
        assertThat(opt1.toString()).isEqualTo(opt2.toString());
    }
}
