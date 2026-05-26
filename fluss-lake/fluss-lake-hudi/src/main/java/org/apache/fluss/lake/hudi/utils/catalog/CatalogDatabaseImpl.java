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

package org.apache.fluss.lake.hudi.utils.catalog;

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Implement of catalog database. */
public class CatalogDatabaseImpl implements CatalogDatabase {
    private final Map<String, String> properties;
    private final String comment;

    public CatalogDatabaseImpl(Map<String, String> properties, @Nullable String comment) {
        this.properties = Preconditions.checkNotNull(properties, "properties cannot be null");
        this.comment = comment;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public String getComment() {
        return this.comment;
    }

    public CatalogDatabase copy() {
        return this.copy(this.getProperties());
    }

    public CatalogDatabase copy(Map<String, String> properties) {
        return new CatalogDatabaseImpl(new HashMap<>(properties), this.comment);
    }

    public Optional<String> getDescription() {
        return Optional.ofNullable(this.comment);
    }

    public Optional<String> getDetailedDescription() {
        return Optional.ofNullable(this.comment);
    }
}
