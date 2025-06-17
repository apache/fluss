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

import com.alibaba.fluss.annotation.PublicStable;
import com.alibaba.fluss.utils.CollectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Lightweight configuration object which stores key/value pairs.
 *
 * @since 0.1
 */
@PublicStable
public class Configuration implements Serializable, ReadableConfig {

    private static final long serialVersionUID = 1L;

    /** The log object used for debugging. */
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);

    /** Stores the concrete key/value pairs of this configuration object. */
    protected final HashMap<String, Object> confData;

    // --------------------------------------------------------------------------------------------

    /** Creates a new empty configuration. */
    public Configuration() {
        this.confData = new HashMap<>();
    }

    /**
     * Creates a new configuration with the copy of the given configuration.
     *
     * @param other The configuration to copy the entries from.
     */
    public Configuration(Configuration other) {
        this.confData = new HashMap<>(other.confData);
    }

    // --------------------------------------------------------------------------------------------

    /** Creates a new configuration that is initialized with the options of the given map. */
    public static Configuration fromMap(Map<String, String> map) {
        final Configuration configuration = new Configuration();
        map.forEach(configuration::setString);
        return configuration;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Returns the class associated with the given key as a string.
     *
     * @param <T> The type of the class to return.
     * @param key The key pointing to the associated value
     * @param defaultValue The optional default value returned if no entry exists
     * @param classLoader The class loader used to resolve the class.
     * @return The value associated with the given key, or the default value, if to entry for the
     *     key exists.
     */
    @SuppressWarnings("unchecked")
    public <T> Class<T> getClass(
            String key, Class<? extends T> defaultValue, ClassLoader classLoader)
            throws ClassNotFoundException {
        Optional<Object> o = getRawValue(key);
        if (!o.isPresent()) {
            return (Class<T>) defaultValue;
        }

        if (o.get().getClass() == String.class) {
            return (Class<T>) Class.forName((String) o.get(), true, classLoader);
        }

        throw new IllegalArgumentException(
                "Configuration cannot evaluate object of class "
                        + o.get().getClass()
                        + " as a class name");
    }

    /**
     * Adds the given key/value pair to the configuration object. The class can be retrieved by
     * invoking {@link #getClass(String, Class, ClassLoader)} if it is in the scope of the class
     * loader on the caller.
     *
     * @param key The key of the pair to be added
     * @param klazz The value of the pair to be added
     * @see #getClass(String, Class, ClassLoader)
     */
    public void setClass(String key, Class<?> klazz) {
        setValueInternal(key, klazz.getName());
    }

    /**
     * Returns the value associated with the given config option as a string.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public String getString(ConfigOption<String> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a string. If no value is mapped
     * under any key of the option, it returns the specified default instead of the option's default
     * value.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public String getString(ConfigOption<String> configOption, String overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setString(String key, String value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setString(ConfigOption<String> key, String value) {
        setValueInternal(key.key(), value);
    }

    /**
     * Returns the value associated with the given config option as an integer.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public int getInt(ConfigOption<Integer> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as an integer. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public int getInt(ConfigOption<Integer> configOption, int overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setInt(String key, int value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setInt(ConfigOption<Integer> key, int value) {
        setValueInternal(key.key(), value);
    }

    /**
     * Returns the value associated with the given config option as a long integer.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public long getLong(ConfigOption<Long> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a long integer. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public long getLong(ConfigOption<Long> configOption, long overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setLong(String key, long value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setLong(ConfigOption<Long> key, long value) {
        setValueInternal(key.key(), value);
    }

    /**
     * Returns the value associated with the given config option as a boolean.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public boolean getBoolean(ConfigOption<Boolean> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a boolean. If no value is mapped
     * under any key of the option, it returns the specified default instead of the option's default
     * value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public boolean getBoolean(ConfigOption<Boolean> configOption, boolean overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setBoolean(String key, boolean value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setBoolean(ConfigOption<Boolean> key, boolean value) {
        setValueInternal(key.key(), value);
    }

    /**
     * Returns the value associated with the given config option as a float.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public float getFloat(ConfigOption<Float> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a float. If no value is mapped
     * under any key of the option, it returns the specified default instead of the option's default
     * value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public float getFloat(ConfigOption<Float> configOption, float overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setFloat(String key, float value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setFloat(ConfigOption<Float> key, float value) {
        setValueInternal(key.key(), value);
    }

    /**
     * Returns the value associated with the given config option as a {@code double}.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public double getDouble(ConfigOption<Double> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a {@code double}. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public double getDouble(ConfigOption<Double> configOption, double overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setDouble(String key, double value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setDouble(ConfigOption<Double> key, double value) {
        setValueInternal(key.key(), value);
    }

    /**
     * Returns the value associated with the given key as a byte array.
     *
     * @param key The key pointing to the associated value.
     * @param defaultValue The default value which is returned in case there is no value associated
     *     with the given key.
     * @return the (default) value associated with the given key.
     */
    public byte[] getBytes(String key, byte[] defaultValue) {
        return getRawValue(key)
                .map(
                        o -> {
                            if (o.getClass().equals(byte[].class)) {
                                return (byte[]) o;
                            } else {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Configuration cannot evaluate value %s as a byte[] value",
                                                o));
                            }
                        })
                .orElse(defaultValue);
    }

    /**
     * Adds the given byte array to the configuration object. If key is <code>null</code> then
     * nothing is added.
     *
     * @param key The key under which the bytes are added.
     * @param bytes The bytes to be added.
     */
    public void setBytes(String key, byte[] bytes) {
        setValueInternal(key, bytes);
    }

    /**
     * Returns the value associated with the given config option as a string.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public String getValue(ConfigOption<?> configOption) {
        return Optional.ofNullable(
                        getRawValueFromOption(configOption).orElseGet(configOption::defaultValue))
                .map(String::valueOf)
                .orElse(null);
    }

    /**
     * Returns the value associated with the given config option as an enum.
     *
     * @param enumClass The return enum class
     * @param configOption The configuration option
     * @throws IllegalArgumentException If the string associated with the given config option cannot
     *     be parsed as a value of the provided enum class.
     */
    public <T extends Enum<T>> T getEnum(
            final Class<T> enumClass, final ConfigOption<String> configOption) {
        checkNotNull(enumClass, "enumClass must not be null");
        checkNotNull(configOption, "configOption must not be null");

        Object rawValue = getRawValueFromOption(configOption).orElseGet(configOption::defaultValue);
        try {
            return ConfigurationUtils.convertToEnum(rawValue, enumClass);
        } catch (IllegalArgumentException ex) {
            final String errorMessage =
                    String.format(
                            "Value for config option %s must be one of %s (was %s)",
                            configOption.key(),
                            Arrays.toString(enumClass.getEnumConstants()),
                            rawValue);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * Returns the value associated with the given config option as a map.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public Map<String, String> getMap(ConfigOption<Map<String, String>> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }
    // --------------------------------------------------------------------------------------------

    /**
     * Returns the keys of all key/value pairs stored inside this configuration object.
     *
     * @return the keys of all key/value pairs stored inside this configuration object
     */
    public Set<String> keySet() {
        synchronized (this.confData) {
            return new HashSet<>(this.confData.keySet());
        }
    }

    /** Adds all entries in this {@code Configuration} to the given {@link Properties}. */
    public void addAllToProperties(Properties props) {
        synchronized (this.confData) {
            props.putAll(confData);
        }
    }

    public void addAll(Configuration other) {
        synchronized (this.confData) {
            synchronized (other.confData) {
                this.confData.putAll(other.confData);
            }
        }
    }

    /**
     * Checks whether there is an entry with the specified key.
     *
     * @param key key of entry
     * @return true if the key is stored, false otherwise
     */
    public boolean containsKey(String key) {
        synchronized (this.confData) {
            return this.confData.containsKey(key);
        }
    }

    /**
     * Checks whether there is an entry for the given config option.
     *
     * @param configOption The configuration option
     * @return <tt>true</tt> if a valid (current or deprecated) key of the config option is stored,
     *     <tt>false</tt> otherwise
     */
    public boolean contains(ConfigOption<?> configOption) {
        synchronized (this.confData) {
            if (this.confData.containsKey(configOption.key())) {
                return true;
            } else {
                for (FallbackKey fallbackKey : configOption.fallbackKeys()) {
                    if (this.confData.containsKey(fallbackKey.getKey())) {
                        loggingFallback(fallbackKey, configOption);
                        return true;
                    }
                }

                return false;
            }
        }
    }

    /**
     * Please check the java doc of {@link #getRawValueFromOption(ConfigOption)}. If no keys are
     * found in {@link Configuration}, default value of the given option will return. Please make
     * sure there will be at least one value available. Otherwise, a NPE will be thrown by Fluss
     * when the value is used.
     *
     * <p>NOTE: current logic is not able to get the default value of the fallback key's
     * ConfigOption, in case the given ConfigOption has no default value. If you want to use
     * fallback key, please make sure its value could be found in {@link Configuration} at runtime.
     *
     * @param option metadata of the option to read
     * @return the value of the given option
     */
    @Override
    public <T> T get(ConfigOption<T> option) {
        return getOptional(option).orElseGet(option::defaultValue);
    }

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        Optional<Object> rawValue = getRawValueFromOption(option);
        Class<?> clazz = option.getClazz();

        try {
            if (option.isList()) {
                return rawValue.map(v -> ConfigurationUtils.convertToList(v, clazz));
            } else {
                return rawValue.map(v -> ConfigurationUtils.convertValue(v, clazz));
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    option.isSensitive()
                            ? String.format("Could not parse value for key '%s'.", option.key())
                            : String.format(
                                    "Could not parse value '%s' for key '%s'.",
                                    rawValue.map(Object::toString).orElse(""), option.key()),
                    e);
        }
    }

    public <T> Configuration set(ConfigOption<T> option, T value) {
        setValueInternal(option.key(), value);
        return this;
    }

    // --------------------------------------------------------------------------------------------

    public Map<String, String> toMap() {
        synchronized (this.confData) {
            Map<String, String> ret =
                    CollectionUtils.newHashMapWithExpectedSize(this.confData.size());
            for (Map.Entry<String, Object> entry : confData.entrySet()) {
                ret.put(entry.getKey(), ConfigurationUtils.convertToString(entry.getValue()));
            }
            return ret;
        }
    }

    /**
     * Removes given config option from the configuration.
     *
     * @param configOption config option to remove
     * @param <T> Type of the config option
     * @return true is config has been removed, false otherwise
     */
    public <T> boolean removeConfig(ConfigOption<T> configOption) {
        synchronized (this.confData) {
            if (this.confData.remove(configOption.key()) != null) {
                return true;
            } else {
                for (FallbackKey fallbackKey : configOption.fallbackKeys()) {
                    if (this.confData.remove(fallbackKey.getKey()) != null) {
                        loggingFallback(fallbackKey, configOption);
                        return true;
                    }
                }

                return false;
            }
        }
    }

    /**
     * Removes given key from the configuration.
     *
     * @param key key of a config option to remove
     * @return true is config has been removed, false otherwise
     */
    public boolean removeKey(String key) {
        synchronized (this.confData) {
            return this.confData.remove(key) != null;
        }
    }

    // --------------------------------------------------------------------------------------------

    <T> void setValueInternal(String key, T value) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }
        if (value == null) {
            throw new NullPointerException("Value must not be null.");
        }

        synchronized (this.confData) {
            this.confData.put(key, value);
        }
    }

    private Optional<Object> getRawValue(String key) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }

        synchronized (this.confData) {
            final Object value = this.confData.get(key);
            if (value != null) {
                return Optional.of(value);
            }

            return Optional.empty();
        }
    }

    /**
     * This method will do the following steps to get the value of a config option:
     *
     * <p>1. get the value from {@link Configuration}. <br>
     * 2. if key is not found, try to get the value with fallback keys from {@link Configuration}
     * <br>
     * 3. if no fallback keys are found, return {@link Optional#empty()}. <br>
     *
     * @return the value of the configuration or {@link Optional#empty()}.
     */
    private Optional<Object> getRawValueFromOption(ConfigOption<?> configOption) {
        Optional<Object> rawValue = getRawValue(configOption.key());

        if (rawValue.isPresent()) {
            return rawValue;
        } else {
            for (FallbackKey fallbackKey : configOption.fallbackKeys()) {
                Optional<Object> fallbackValue = getRawValue(fallbackKey.getKey());
                if (fallbackValue.isPresent()) {
                    loggingFallback(fallbackKey, configOption);
                    return fallbackValue;
                }
            }

            return Optional.empty();
        }
    }

    private void loggingFallback(FallbackKey fallbackKey, ConfigOption<?> configOption) {
        if (fallbackKey.isDeprecated()) {
            LOG.warn(
                    "Config uses deprecated configuration key '{}' instead of proper key '{}'",
                    fallbackKey.getKey(),
                    configOption.key());
        } else {
            LOG.info(
                    "Config uses fallback configuration key '{}' instead of key '{}'",
                    fallbackKey.getKey(),
                    configOption.key());
        }
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        int hash = 0;
        for (String s : this.confData.keySet()) {
            hash ^= s.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof Configuration) {
            Map<String, Object> otherConf = ((Configuration) obj).confData;

            for (Map.Entry<String, Object> e : this.confData.entrySet()) {
                Object thisVal = e.getValue();
                Object otherVal = otherConf.get(e.getKey());

                if (!thisVal.getClass().equals(byte[].class)) {
                    if (!thisVal.equals(otherVal)) {
                        return false;
                    }
                } else if (otherVal.getClass().equals(byte[].class)) {
                    if (!Arrays.equals((byte[]) thisVal, (byte[]) otherVal)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }

            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return this.confData.toString();
    }
}
