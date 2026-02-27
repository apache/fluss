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

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** To describe the changes of the properties of a database. */
public class DatabasePropertyChanges {

    public final Map<String, String> customPropertiesToSet;
    public final Set<String> customPropertiesToReset;

    public final String commentToSet;

    private final boolean commentChanged;

    protected DatabasePropertyChanges(
            Map<String, String> customPropertiesToSet,
            Set<String> customPropertiesToReset,
            @Nullable String commentToSet,
            boolean commentChanged) {
        this.customPropertiesToSet = customPropertiesToSet;
        this.customPropertiesToReset = customPropertiesToReset;
        this.commentToSet = commentToSet;
        this.commentChanged = commentChanged;
    }

    public boolean isCommentChanged() {
        return commentChanged;
    }

    public static DatabasePropertyChanges.Builder builder() {
        return new DatabasePropertyChanges.Builder();
    }

    /** The builder for {@link DatabasePropertyChanges}. */
    public static class Builder {
        private final Map<String, String> customPropertiesToSet = new HashMap<>();
        private final Set<String> customPropertiesToReset = new HashSet<>();

        private String commentToSet = null;
        private boolean commentChanged = false;

        public void setCustomProperty(String key, String value) {
            customPropertiesToSet.put(key, value);
        }

        public void resetCustomProperty(String key) {
            customPropertiesToReset.add(key);
        }

        public void setComment(@Nullable String comment) {
            this.commentToSet = comment;
            this.commentChanged = true;
        }

        public DatabasePropertyChanges build() {
            return new DatabasePropertyChanges(
                    customPropertiesToSet, customPropertiesToReset, commentToSet, commentChanged);
        }
    }
}
