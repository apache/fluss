/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.entity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** To describe the changes of the properties of a table. */
public class TablePropertyChanges {

    public final Map<String, String> tablePropertiesToSet;
    public final Set<String> tablePropertiesToReset;

    public final Map<String, String> customPropertiesToSet;
    public final Set<String> customPropertiesToReset;

    protected TablePropertyChanges(
            Map<String, String> tablePropertiesToSet,
            Set<String> tablePropertiesToReset,
            Map<String, String> customPropertiesToSet,
            Set<String> customPropertiesToReset) {
        this.tablePropertiesToSet = tablePropertiesToSet;
        this.tablePropertiesToReset = tablePropertiesToReset;
        this.customPropertiesToSet = customPropertiesToSet;
        this.customPropertiesToReset = customPropertiesToReset;
    }

    public Set<String> tableKeysToChange() {
        Set<String> keys = new HashSet<>(tablePropertiesToSet.keySet());
        keys.addAll(tablePropertiesToReset);
        return keys;
    }

    public Set<String> customKeysToChange() {
        Set<String> keys = new HashSet<>(customPropertiesToSet.keySet());
        keys.addAll(customPropertiesToReset);
        return keys;
    }

    public static TablePropertyChanges.Builder builder() {
        return new TablePropertyChanges.Builder();
    }

    /** The builder for {@link TablePropertyChanges}. */
    public static class Builder {
        private final Map<String, String> tablePropertiesToSet = new HashMap<>();
        private final Set<String> tablePropertiesToReset = new HashSet<>();

        private final Map<String, String> customPropertiesToSet = new HashMap<>();
        private final Set<String> customPropertiesToReset = new HashSet<>();

        public void setTableProperty(String key, String value) {
            tablePropertiesToSet.put(key, value);
        }

        public void resetTableProperty(String key) {
            tablePropertiesToReset.add(key);
        }

        public void setCustomProperty(String key, String value) {
            customPropertiesToSet.put(key, value);
        }

        public void resetCustomProperty(String key) {
            customPropertiesToReset.add(key);
        }

        public TablePropertyChanges build() {
            return new TablePropertyChanges(
                    tablePropertiesToSet,
                    tablePropertiesToReset,
                    customPropertiesToSet,
                    customPropertiesToReset);
        }
    }
}
