/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.docs;

import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.MemorySize;

import java.time.Duration;

/** Utility class for formatting configuration options into human-readable documentation. */
public class ConfigDocUtils {

    /**
     * Formats the default value of a {@link ConfigOption} for documentation purposes.
     *
     * @param option The configuration option to format.
     * @return A string representation of the default value.
     */
    public static String formatDefaultValue(ConfigOption<?> option) {
        Object value = option.defaultValue();

        if (value == null) {
            return "none";
        }

        // Handle Duration: Convert ISO-8601 (PT15M) to human-readable (15 min).
        if (value instanceof Duration) {
            Duration d = (Duration) value;
            long seconds = d.getSeconds(); // Use Java 8 compatible method.

            if (seconds == 0) {
                return "0 s";
            }
            if (seconds >= 3600 && seconds % 3600 == 0) {
                return (seconds / 3600) + " hours";
            }
            if (seconds >= 60 && seconds % 60 == 0) {
                return (seconds / 60) + " min";
            }
            return seconds + " s";
        }

        // Handle MemorySize: Uses internal toString() for human-readable units (e.g., 64 mb).
        if (value instanceof MemorySize) {
            return value.toString();
        }

        // Handle Strings: Specifically check for empty values.
        if (value instanceof String && ((String) value).isEmpty()) {
            return "(empty)";
        }

        return value.toString();
    }
}
