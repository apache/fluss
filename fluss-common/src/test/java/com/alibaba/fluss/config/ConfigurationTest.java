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

package com.alibaba.fluss.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** This class contains test for the configuration package. */
public class ConfigurationTest {

    private static final ConfigOption<String> STRING_OPTION =
            ConfigBuilder.key("test-string-key").stringType().noDefaultValue();
    private static final ConfigOption<Integer> INTEGER_OPTION =
            ConfigBuilder.key("test-integer-key").intType().noDefaultValue();
    private static final ConfigOption<Long> LONG_OPTION =
            ConfigBuilder.key("test-long-key").longType().defaultValue(4716238L);
    private static final ConfigOption<Float> FLOAT_OPTION =
            ConfigBuilder.key("test-float-key").floatType().noDefaultValue();
    private static final ConfigOption<Double> DOUBLE_OPTION =
            ConfigBuilder.key("test-double-key").doubleType().noDefaultValue();
    private static final ConfigOption<Boolean> BOOLEAN_OPTION =
            ConfigBuilder.key("test-boolean-key").booleanType().defaultValue(true);
    private static final ConfigOption<MemorySize> MEMORY_OPTION =
            ConfigBuilder.key("test-memory-key")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64 kb"));
    private static final ConfigOption<List<String>> LIST_STRING_OPTION =
            ConfigBuilder.key("test-list-key").stringType().asList().noDefaultValue();
    private static final ConfigOption<Map<String, String>> MAP_OPTION =
            ConfigBuilder.key("test-map-key").mapType().noDefaultValue();
    private static final ConfigOption<Duration> DURATION_OPTION =
            ConfigBuilder.key("test-duration-key").durationType().noDefaultValue();
    private static final ConfigOption<Password> SECRET_OPTION =
            ConfigBuilder.key("secret").passwordType().noDefaultValue();

    private static final Map<String, String> PROPERTIES_MAP = new HashMap<>();

    static {
        PROPERTIES_MAP.put("prop1", "value1");
        PROPERTIES_MAP.put("prop2", "12");
    }

    @Test
    void testGetterAndSetter() throws Exception {
        Configuration conf = new Configuration();

        conf.setString(STRING_OPTION.key(), "value1");
        assertThat(conf.getString(STRING_OPTION)).isEqualTo("value1");
        conf.setString(STRING_OPTION, "value2");
        assertThat(conf.getString(STRING_OPTION)).isEqualTo("value2");

        conf.setInt(INTEGER_OPTION.key(), 100);
        assertThat(conf.getInt(INTEGER_OPTION)).isEqualTo(100);
        conf.setString(INTEGER_OPTION.key(), "1000");
        assertThat(conf.getInt(INTEGER_OPTION)).isEqualTo(1000);

        conf.setLong(LONG_OPTION.key(), 478236947162389746L);
        assertThat(conf.getLong(LONG_OPTION)).isEqualTo(478236947162389746L);
        conf.setString(LONG_OPTION.key(), "-47823692389746");
        assertThat(conf.getLong(LONG_OPTION)).isEqualTo(-47823692389746L);

        conf.setFloat(FLOAT_OPTION.key(), 3.1415926f);
        assertThat(conf.getFloat(FLOAT_OPTION)).isEqualTo(3.1415926f);
        conf.setString(FLOAT_OPTION.key(), "-3.1415926");
        assertThat(conf.getFloat(FLOAT_OPTION)).isEqualTo(-3.1415926f);

        conf.setDouble(DOUBLE_OPTION.key(), Math.E);
        assertThat(conf.getDouble(DOUBLE_OPTION)).isEqualTo(Math.E);
        conf.setString(DOUBLE_OPTION.key(), "123123.2312");
        assertThat(conf.getDouble(DOUBLE_OPTION)).isEqualTo(123123.2312d);

        conf.setBoolean(BOOLEAN_OPTION.key(), false);
        assertThat(conf.getBoolean(BOOLEAN_OPTION)).isEqualTo(false);
        conf.setString(BOOLEAN_OPTION.key(), "true");
        assertThat(conf.getBoolean(BOOLEAN_OPTION)).isEqualTo(true);

        conf.set(MEMORY_OPTION, MemorySize.parse("128 kb"));
        assertThat(conf.get(MEMORY_OPTION).getBytes()).isEqualTo(128 * 1024);
        conf.setString(MEMORY_OPTION.key(), "12mb");
        assertThat(conf.get(MEMORY_OPTION).getBytes()).isEqualTo(12 * 1024 * 1024);

        conf.set(DURATION_OPTION, Duration.ofMillis(3));
        assertThat(conf.get(DURATION_OPTION).toMillis()).isEqualTo(3);
        conf.setString(DURATION_OPTION.key(), "3 s");
        assertThat(conf.get(DURATION_OPTION).toMillis()).isEqualTo(3000);

        conf.setBytes("test-bytes-key", new byte[] {1, 2, 3, 4, 5});
        assertThat(conf.getBytes("test-bytes-key", new byte[0]).length).isEqualTo(5);

        conf.setClass("test-class-key", this.getClass());
        assertThat(
                        conf.getClass(
                                "test-class-key",
                                this.getClass(),
                                this.getClass().getClassLoader()))
                .isEqualTo(this.getClass());

        // test getter with default value.
        conf = new Configuration();
        assertThat(conf.getString(STRING_OPTION, "value3")).isEqualTo("value3");
        assertThat(conf.getInt(INTEGER_OPTION, 1)).isEqualTo(1);
        assertThat(conf.getLong(LONG_OPTION, 200L)).isEqualTo(200L);
        assertThat(conf.getFloat(FLOAT_OPTION, 3.15f)).isEqualTo(3.15f);
        assertThat(conf.getDouble(DOUBLE_OPTION, 1.1d)).isEqualTo(1.1d);
        assertThat(conf.getBoolean(BOOLEAN_OPTION, false)).isEqualTo(false);
        assertThat(conf.get(MEMORY_OPTION).getBytes()).isEqualTo(65536L);
    }

    @Test
    void testCopyConstructor() {
        Configuration conf1 = new Configuration();
        conf1.setString(STRING_OPTION, "value");

        Configuration conf2 = new Configuration(conf1);
        conf2.setString(STRING_OPTION, "another value");

        assertThat(conf1.getString(STRING_OPTION)).isEqualTo("value");
        assertThat(conf2.getString(STRING_OPTION)).isEqualTo("another value");
    }

    @Test
    void testAddAll() {
        Configuration conf1 = new Configuration();
        conf1.setString(STRING_OPTION, "value");
        conf1.setInt(INTEGER_OPTION, 1);

        Configuration conf2 = new Configuration();
        conf2.addAll(conf1);

        assertThat(conf2.keySet().size()).isEqualTo(2);
        assertThat(conf2.getString(STRING_OPTION)).isEqualTo("value");
        assertThat(conf2.getInt(INTEGER_OPTION)).isEqualTo(1);
    }

    @Test
    void testAddAllToProperties() {
        Configuration conf1 = new Configuration();
        conf1.setString(STRING_OPTION, "value");
        conf1.setInt(INTEGER_OPTION, 1);

        Properties props = new Properties();
        conf1.addAllToProperties(props);

        assertThat(props.size()).isEqualTo(2);
        assertThat(props.getProperty(STRING_OPTION.key())).isEqualTo("value");
        assertThat(props.get(INTEGER_OPTION.key())).isEqualTo(1);
    }

    @Test
    void testOptionWithDefault() {
        Configuration conf = new Configuration();
        conf.setInt("int-key", 11);
        conf.setString("string-key", "abc");

        ConfigOption<String> presentStringOption =
                ConfigBuilder.key("string-key").stringType().defaultValue("my-beautiful-default");
        ConfigOption<Integer> presentIntOption =
                ConfigBuilder.key("int-key").intType().defaultValue(87);

        assertThat(conf.getString(presentStringOption)).isEqualTo("abc");
        assertThat(conf.getValue(presentStringOption)).isEqualTo("abc");
        assertThat(conf.getInt(presentIntOption)).isEqualTo(11);
        assertThat(conf.getValue(presentIntOption)).isEqualTo("11");

        // test getting default when no value is present.
        ConfigOption<String> stringOption =
                ConfigBuilder.key("test").stringType().defaultValue("my-beautiful-default");
        ConfigOption<Integer> intOption = ConfigBuilder.key("test2").intType().defaultValue(87);

        assertThat(conf.getString(stringOption)).isEqualTo("my-beautiful-default");
        assertThat(conf.getValue(stringOption)).isEqualTo("my-beautiful-default");

        // overriding the default should work.
        assertThat(conf.getString(stringOption, "override")).isEqualTo("override");

        // getting a primitive with a default value should work
        assertThat(conf.getInt(intOption)).isEqualTo(87);
        assertThat(conf.getValue(intOption)).isEqualTo("87");
    }

    @Test
    void testOptionWithNoDefault() {
        Configuration conf = new Configuration();
        conf.setInt("int-key", 11);
        conf.setString("string-key", "abc");

        ConfigOption<String> presentStringOption =
                ConfigBuilder.key("string-key").stringType().noDefaultValue();
        assertThat(conf.getString(presentStringOption)).isEqualTo("abc");
        assertThat(conf.getValue(presentStringOption)).isEqualTo("abc");

        // test getting default when no value is present.
        ConfigOption<String> stringOption = ConfigBuilder.key("test").stringType().noDefaultValue();
        assertThat(conf.getString(stringOption)).isNull();
        assertThat(conf.getValue(stringOption)).isNull();

        // overriding the null default should work.
        assertThat(conf.getString(stringOption, "override")).isEqualTo("override");
    }

    @Test
    void testFromMap() {
        Map<String, String> map = new HashMap<>();
        map.put(STRING_OPTION.key(), "value");
        map.put(INTEGER_OPTION.key(), "1");

        Configuration configuration = Configuration.fromMap(map);

        assertThat(configuration.get(STRING_OPTION)).isEqualTo("value");
        assertThat(configuration.getString(STRING_OPTION)).isEqualTo("value");
        assertThat(configuration.get(INTEGER_OPTION)).isEqualTo(1);
        assertThat(configuration.getInt(INTEGER_OPTION)).isEqualTo(1);
    }

    @Test
    void testDeprecatedKeys() {
        Configuration conf = new Configuration();
        conf.setInt("the-key", 11);
        conf.setInt("old-key", 12);
        conf.setInt("older-key", 13);

        ConfigOption<Integer> matchesFirst =
                ConfigBuilder.key("the-key")
                        .intType()
                        .defaultValue(-1)
                        .withDeprecatedKeys("old-key", "older-key");

        ConfigOption<Integer> matchesSecond =
                ConfigBuilder.key("does-not-exist")
                        .intType()
                        .defaultValue(-1)
                        .withDeprecatedKeys("old-key", "older-key");

        ConfigOption<Integer> matchesThird =
                ConfigBuilder.key("does-not-exist")
                        .intType()
                        .defaultValue(-1)
                        .withDeprecatedKeys("foo", "older-key");

        ConfigOption<Integer> notContained =
                ConfigBuilder.key("does-not-exist")
                        .intType()
                        .defaultValue(-1)
                        .withDeprecatedKeys("not-there", "also-not-there");

        assertThat(conf.getInt(matchesFirst)).isEqualTo(11);
        assertThat(conf.getInt(matchesSecond)).isEqualTo(12);
        assertThat(conf.getInt(matchesThird)).isEqualTo(13);
        assertThat(conf.getInt(notContained)).isEqualTo(-1);
    }

    @Test
    void testFallbackKeys() {
        Configuration conf = new Configuration();
        conf.setInt("the-key", 11);
        conf.setInt("old-key", 12);
        conf.setInt("older-key", 13);

        ConfigOption<Integer> matchesFirst =
                ConfigBuilder.key("the-key")
                        .intType()
                        .defaultValue(-1)
                        .withFallbackKeys("old-key", "older-key");

        ConfigOption<Integer> matchesSecond =
                ConfigBuilder.key("does-not-exist")
                        .intType()
                        .defaultValue(-1)
                        .withFallbackKeys("old-key", "older-key");

        ConfigOption<Integer> matchesThird =
                ConfigBuilder.key("does-not-exist")
                        .intType()
                        .defaultValue(-1)
                        .withFallbackKeys("foo", "older-key");

        ConfigOption<Integer> notContained =
                ConfigBuilder.key("does-not-exist")
                        .intType()
                        .defaultValue(-1)
                        .withFallbackKeys("not-there", "also-not-there");

        assertThat(conf.getInt(matchesFirst)).isEqualTo(11);
        assertThat(conf.getInt(matchesSecond)).isEqualTo(12);
        assertThat(conf.getInt(matchesThird)).isEqualTo(13);
        assertThat(conf.getInt(notContained)).isEqualTo(-1);
    }

    @Test
    void testFallbackAndDeprecatedKeys() {
        final ConfigOption<Integer> fallback =
                ConfigBuilder.key("fallback").intType().defaultValue(-1);
        final ConfigOption<Integer> deprecated =
                ConfigBuilder.key("deprecated").intType().defaultValue(-1);

        final ConfigOption<Integer> mainOption =
                ConfigBuilder.key("main")
                        .intType()
                        .defaultValue(-1)
                        .withFallbackKeys(fallback.key())
                        .withDeprecatedKeys(deprecated.key());

        final Configuration fallbackConf = new Configuration();
        fallbackConf.setInt(fallback, 1);
        assertThat(fallbackConf.get(mainOption)).isEqualTo(1);

        final Configuration deprecatedConf = new Configuration();
        deprecatedConf.setInt(deprecated, 2);
        assertThat(deprecatedConf.getInt(mainOption)).isEqualTo(2);

        // Reverse declaration of fallback and deprecated keys, fallback keys should always be used
        // first
        final Configuration deprecatedAndFallBackConfig = new Configuration();
        deprecatedAndFallBackConfig.setInt(deprecated, 1);
        deprecatedAndFallBackConfig.setInt(fallback, 2);
        assertThat(deprecatedAndFallBackConfig.getInt(mainOption)).isEqualTo(2);
    }

    @Test
    void testRemoveConf() {
        Configuration conf = new Configuration();
        conf.setInt("a", 1);
        conf.setInt("b", 2);

        ConfigOption<Integer> validOption = ConfigBuilder.key("a").intType().defaultValue(-1);

        ConfigOption<Integer> deprecatedOption =
                ConfigBuilder.key("c").intType().defaultValue(-1).withDeprecatedKeys("d", "b");

        ConfigOption<Integer> unExistedOption =
                ConfigBuilder.key("e").intType().defaultValue(-1).withDeprecatedKeys("f", "g", "j");

        assertThat(conf.keySet().size()).isEqualTo(2);
        assertThat(conf.removeConfig(validOption)).isTrue();
        assertThat(conf.contains(validOption)).isFalse();
        assertThat(conf.keySet().size()).isEqualTo(1);

        assertThat(conf.contains(deprecatedOption)).isTrue();
        assertThat(conf.removeConfig(deprecatedOption)).isTrue();
        assertThat(conf.contains(deprecatedOption)).isFalse();
        assertThat(conf.keySet().size()).isEqualTo(0);

        assertThat(conf.removeConfig(unExistedOption)).isFalse();
    }

    @Test
    void testRemoveKey() {
        Configuration conf = new Configuration();
        String key1 = "a";
        conf.setInt(key1, 42);
        conf.setInt("e.f", 1337);

        assertThat(conf.removeKey("not-existing-key")).isFalse();
        assertThat(conf.removeKey(key1)).isTrue();
        assertThat(conf.containsKey(key1)).isFalse();
    }

    @Test
    void testShouldParseValidStringToEnum() {
        final Configuration conf = new Configuration();
        conf.setString(STRING_OPTION.key(), TestEnum.VALUE1.toString());

        final TestEnum parsedEnumValue = conf.getEnum(TestEnum.class, STRING_OPTION);
        assertThat(parsedEnumValue).isEqualTo(TestEnum.VALUE1);
    }

    @Test
    void testShouldParseValidStringToEnumIgnoringCase() {
        final Configuration conf = new Configuration();
        conf.setString(STRING_OPTION.key(), TestEnum.VALUE1.toString().toLowerCase());

        final TestEnum parsedEnumValue = conf.getEnum(TestEnum.class, STRING_OPTION);
        assertThat(parsedEnumValue).isEqualTo(TestEnum.VALUE1);
    }

    @Test
    void testThrowsExceptionIfTryingToParseInvalidStringForEnum() {
        final Configuration conf = new Configuration();
        final String invalidValueForTestEnum = "InvalidValueForTestEnum";
        conf.setString(STRING_OPTION.key(), invalidValueForTestEnum);

        assertThatThrownBy(() -> conf.getEnum(TestEnum.class, STRING_OPTION))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testToMap() {
        final Configuration conf = new Configuration();
        final String listValues = "value1,value2,value3";
        conf.set(LIST_STRING_OPTION, Arrays.asList(listValues.split(",")));

        final String mapValues = "key1:value1,key2:value2";
        conf.set(
                MAP_OPTION,
                Arrays.stream(mapValues.split(","))
                        .collect(Collectors.toMap(e -> e.split(":")[0], e -> e.split(":")[1])));

        final Duration duration = Duration.ofMillis(3000);
        conf.set(DURATION_OPTION, duration);

        assertThat(conf.toMap().get(LIST_STRING_OPTION.key())).isEqualTo(listValues);
        assertThat(conf.toMap().get(MAP_OPTION.key())).isEqualTo(mapValues);
        assertThat(conf.toMap().get(DURATION_OPTION.key())).isEqualTo("3 s");
    }

    @Test
    void testMapNotContained() {
        final Configuration conf = new Configuration();

        assertThat(conf.getOptional(MAP_OPTION).isPresent()).isFalse();
        assertThat(conf.contains(MAP_OPTION)).isFalse();
    }

    @Test
    void testMap() {
        final Configuration cfg = new Configuration();
        cfg.set(MAP_OPTION, PROPERTIES_MAP);

        assertThat(cfg.get(MAP_OPTION)).isEqualTo(PROPERTIES_MAP);
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
    }

    @Test
    void testPasswordType() {
        final Configuration cfg = new Configuration();
        final String secretValue = "secret_value";
        cfg.setString(SECRET_OPTION.key(), secretValue);

        assertThat(cfg.get(SECRET_OPTION)).isEqualTo(new Password(secretValue));
        assertThat(cfg.get(SECRET_OPTION).toString()).isEqualTo("******");
        assertThat(cfg.get(SECRET_OPTION).value()).isEqualTo(secretValue);
    }

    @Test
    void testPasswordParserErrorDoesNotLeakSensitiveData() {
        assertThat(SECRET_OPTION.isSensitive()).isTrue();

        final Configuration cfg = new Configuration();
        cfg.setValueInternal(SECRET_OPTION.key(), new ToStringError());

        assertThatThrownBy(() -> cfg.get(SECRET_OPTION))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Could not parse value for key 'secret'.");
    }

    @Test
    void testPasswordListParserErrorDoesNotLeakSensitiveData() {
        ConfigOption<List<Password>> secret =
                ConfigBuilder.key("secret").passwordType().asList().noDefaultValue();

        assertThat(secret.isSensitive()).isTrue();

        final Configuration cfg = new Configuration();
        // missing closing quote
        cfg.setString(secret.key(), "'secret_value");

        assertThatThrownBy(() -> cfg.get(secret))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("secret_value");
    }

    // --------------------------------------------------------------------------------------------
    // Test classes
    // --------------------------------------------------------------------------------------------

    enum TestEnum {
        VALUE1,
        VALUE2
    }

    static class ToStringError {
        @Override
        public String toString() {
            throw new RuntimeException("Test exception");
        }
    }
}
