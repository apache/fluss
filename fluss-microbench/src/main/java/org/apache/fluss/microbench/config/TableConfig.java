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

package org.apache.fluss.microbench.config;

import java.util.List;
import java.util.Map;

/** Table schema and storage configuration. */
public class TableConfig {

    private String name;
    private List<ColumnConfig> columns;
    private List<String> primaryKey;
    private String mergeEngine;
    private Integer buckets;
    private List<String> bucketKeys;
    private String logFormat;
    private String kvFormat;
    private Map<String, String> properties;

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ColumnConfig> columns() {
        return columns;
    }

    public void setColumns(List<ColumnConfig> columns) {
        this.columns = columns;
    }

    public List<String> primaryKey() {
        return primaryKey;
    }

    public boolean hasPrimaryKey() {
        return primaryKey != null && !primaryKey.isEmpty();
    }

    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String mergeEngine() {
        return mergeEngine;
    }

    public void setMergeEngine(String mergeEngine) {
        this.mergeEngine = mergeEngine;
    }

    public Integer buckets() {
        return buckets;
    }

    public void setBuckets(Integer buckets) {
        this.buckets = buckets;
    }

    public List<String> bucketKeys() {
        return bucketKeys;
    }

    public void setBucketKeys(List<String> bucketKeys) {
        this.bucketKeys = bucketKeys;
    }

    public String logFormat() {
        return logFormat;
    }

    public void setLogFormat(String logFormat) {
        this.logFormat = logFormat;
    }

    public String kvFormat() {
        return kvFormat;
    }

    public void setKvFormat(String kvFormat) {
        this.kvFormat = kvFormat;
    }

    public Map<String, String> properties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
