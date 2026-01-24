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

import org.apache.fluss.config.MemorySize;

import java.time.Duration;

/** Utils for generating configuration documentation. */
public class ConfigDocUtils {

    public static String formatDefaultValue(Object value) {
        if (value == null) {
            return "none";
        }
        if (value instanceof Duration) {
            return formatDuration((Duration) value);
        }
        if (value instanceof MemorySize) {
            // No need to cast to MemorySize again, it's redundant
            return value.toString();
        }
        return value.toString();
    }

    private static String formatDuration(Duration d) {
        long seconds = d.getSeconds();
        if (seconds == 0) {
            return "0";
        }
        if (seconds % 3600 == 0) {
            return (seconds / 3600) + " hours";
        }
        if (seconds % 60 == 0) {
            return (seconds / 60) + " min";
        }
        return seconds + " s";
    }
}
