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

/** Utils for generating configuration documentation. */
public class ConfigDocUtils {

    public static String formatDefaultValue(ConfigOption<?> option) {
        Object value = option.defaultValue();

        if (value == null) {
            return "none";
        }

        // Handle Duration (PT15M -> 15 min)
        if (value instanceof Duration) {
            Duration d = (Duration) value;
            if (d.isZero()) {
                return "0";
            }
            if (d.toHours() > 0 && d.toMinutes() % 60 == 0) {
                return d.toHours() + " hours";
            }
            if (d.toMinutes() > 0 && d.toSeconds() % 60 == 0) {
                return d.toMinutes() + " min";
            }
            return d.getSeconds() + " s";
        }

        // Handle MemorySize (Uses Fluss's built-in human-readable conversion)
        if (value instanceof MemorySize) {
            return value.toString();
        }

        // Handle Empty Strings (like client.id)
        if (value instanceof String && ((String) value).isEmpty()) {
            return "(empty)";
        }

        return value.toString();
    }
}
