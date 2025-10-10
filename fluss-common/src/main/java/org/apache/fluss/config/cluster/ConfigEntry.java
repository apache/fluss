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

package org.apache.fluss.config.cluster;

import javax.annotation.Nullable;

import java.util.Objects;

/** Configuration entry for cluster. */
public class ConfigEntry {
    private final String key;
    @Nullable private final String value;
    private final ConfigSource source;

    /**
     * Create a configuration with the provided values.
     *
     * @param key the non-null config name
     * @param value the config value or null
     * @param source the source of this config entr
     */
    public ConfigEntry(String key, @Nullable String value, ConfigSource source) {
        Objects.requireNonNull(key, "name should not be null");
        this.key = key;
        this.value = value;
        this.source = source;
    }

    /** Return the config key. */
    public String key() {
        return key;
    }

    /**
     * Return the value or null. Null is returned if the config is unset or if isSensitive is true.
     */
    public @Nullable String value() {
        return value;
    }

    /** Return the source of this configuration entry. */
    public ConfigSource source() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConfigEntry that = (ConfigEntry) o;

        return this.key.equals(that.key) && this.value != null
                ? this.value.equals(that.value)
                : that.value == null && this.source == that.source;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, source);
    }

    @Override
    public String toString() {
        return "ConfigEntry(" + "name=" + key + ", value=" + value + ", source=" + source + ")";
    }

    /** Source of configuration entries. */
    public enum ConfigSource {
        DYNAMIC_SERVER_CONFIG, // dynamic server config that is configured for all servers in the
        // cluster
        INITIAL_SERVER_CONFIG, // initial server config provided as server properties at start
        // up(e.g. server.yaml file)
    }
}
