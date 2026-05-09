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
import java.util.Collection;
import java.util.stream.Collectors;

/** Utility class for formatting configuration options into human-readable documentation. */
public class ConfigDocUtils {

    /** The threshold for considering a duration or memory size as infinite. */
    private static final long INFINITE_THRESHOLD = 9223372036L;

    public static String formatDefaultValue(ConfigOption<?> option) {
        Object value = option.defaultValue();

        if (value == null) {
            return "none";
        }

        if (value instanceof Duration) {
            return formatDuration((Duration) value);
        }

        if (value instanceof MemorySize) {
            MemorySize mem = (MemorySize) value;
            // Handle max values to avoid showing raw bytes
            if (mem.getBytes() >= Long.MAX_VALUE || mem.getBytes() < 0) {
                return "infinite";
            }
            return value.toString().toLowerCase();
        }

        if (value instanceof Collection) {
            Collection<?> col = (Collection<?>) value;
            if (col.isEmpty()) {
                return "none";
            }
            return "[" + col.stream().map(String::valueOf).collect(Collectors.joining(", ")) + "]";
        }

        if (value instanceof String && ((String) value).isEmpty()) {
            return "(empty)";
        }

        return String.valueOf(value);
    }

    private static String formatDuration(Duration d) {
        long seconds = d.getSeconds();
        int nanos = d.getNano();

        if (seconds >= INFINITE_THRESHOLD || seconds < 0) {
            return "infinite";
        }
        if (seconds == 0 && nanos == 0) {
            return "0 s";
        }
        // Handle sub-second durations
        if (seconds == 0) {
            long millis = nanos / 1_000_000;
            if (millis > 0 && nanos % 1_000_000 == 0) {
                return millis + " ms";
            }
            return nanos + " ns";
        }
        if (seconds >= 86400 && seconds % 86400 == 0) {
            long days = seconds / 86400;
            return days + (days == 1 ? " day" : " days");
        }
        if (seconds >= 3600 && seconds % 3600 == 0) {
            long hours = seconds / 3600;
            return hours + (hours == 1 ? " hour" : " hours");
        }
        if (seconds >= 60 && seconds % 60 == 0) {
            long mins = seconds / 60;
            return mins + " min";
        }
        return seconds + " s";
    }
}
