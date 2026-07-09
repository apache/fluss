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

package org.apache.fluss.server.config;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Redacts map config values by preserving keys and masking values. */
class MapConfigRedactor implements ConfigRedactor {

    static final String REDACTED_VALUE = "******";

    private final String configKey;

    MapConfigRedactor(String configKey) {
        this.configKey = configKey;
    }

    @Override
    public boolean supports(String configKey) {
        return this.configKey.equals(configKey);
    }

    @Override
    public @Nullable String redact(@Nullable String value) {
        if (value == null) {
            return null;
        }

        List<String> redactedEntries = new ArrayList<>();
        for (String rawEntry : value.split(",")) {
            String entry = rawEntry.trim();
            if (entry.isEmpty()) {
                continue;
            }
            int separatorIndex = entry.indexOf(':');
            if (separatorIndex <= 0) {
                return REDACTED_VALUE;
            }
            redactedEntries.add(entry.substring(0, separatorIndex) + ":" + REDACTED_VALUE);
        }
        return String.join(",", redactedEntries);
    }
}
