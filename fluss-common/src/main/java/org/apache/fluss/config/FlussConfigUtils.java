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

package org.apache.fluss.config;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utilities of Fluss {@link ConfigOptions}. */
@Internal
public class FlussConfigUtils {

    public static final Map<String, ConfigOption<?>> TABLE_OPTIONS;
    public static final Map<String, ConfigOption<?>> CLIENT_OPTIONS;
    public static final String TABLE_PREFIX = "table.";
    public static final String CLIENT_PREFIX = "client.";
    public static final String CLIENT_SECURITY_PREFIX = "client.security.";

    public static final List<String> ALTERABLE_TABLE_CONFIG;

    static {
        TABLE_OPTIONS = extractConfigOptions("table.");
        CLIENT_OPTIONS = extractConfigOptions("client.");
        ALTERABLE_TABLE_CONFIG = Collections.emptyList();
    }

    public static boolean isTableStorageConfig(String key) {
        return key.startsWith(TABLE_PREFIX);
    }

    @VisibleForTesting
    static Map<String, ConfigOption<?>> extractConfigOptions(String prefix) {
        Map<String, ConfigOption<?>> options = new HashMap<>();
        Field[] fields = ConfigOptions.class.getFields();
        // use Java reflection to collect all options matches the prefix
        for (Field field : fields) {
            if (!ConfigOption.class.isAssignableFrom(field.getType())) {
                continue;
            }
            try {
                ConfigOption<?> configOption = (ConfigOption<?>) field.get(null);
                if (configOption.key().startsWith(prefix)) {
                    options.put(configOption.key(), configOption);
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(
                        "Unable to extract ConfigOption fields from ConfigOptions class.", e);
            }
        }
        return options;
    }
}
