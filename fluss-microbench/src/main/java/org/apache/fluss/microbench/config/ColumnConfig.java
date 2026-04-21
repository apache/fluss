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

import java.util.Map;

/**
 * Column definition within a table.
 *
 * <p>The {@code agg} field is stored as raw {@link Object} during YAML parsing to support both
 * simple string form and expanded map form. It is post-processed into an {@link AggConfig} by
 * {@link ScenarioConfig#parse(String)}.
 */
public class ColumnConfig {

    private String name;
    private String type;
    private Object rawAgg;
    private AggConfig aggConfig;

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String type() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /** Returns the resolved {@link AggConfig}, or {@code null} if no aggregation is defined. */
    public AggConfig agg() {
        return aggConfig;
    }

    /**
     * Called by SnakeYAML. Accepts either a String ("SUM") or a Map ({function: LISTAGG, args:
     * ...}).
     */
    public void setAgg(Object agg) {
        this.rawAgg = agg;
    }

    /** Returns the raw agg value as parsed by SnakeYAML (String or Map). */
    Object rawAgg() {
        return rawAgg;
    }

    /** Resolves the raw agg value into an {@link AggConfig}. Called during post-processing. */
    // SnakeYAML deserializes map values as raw Map<String, Object>;
    // the cast is safe because we control the YAML schema.
    @SuppressWarnings("unchecked")
    void resolveAgg() {
        if (rawAgg == null) {
            return;
        }
        if (rawAgg instanceof String) {
            aggConfig = new AggConfig((String) rawAgg, null);
        } else if (rawAgg instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) rawAgg;
            String function = (String) map.get("function");
            Map<String, String> args = null;
            Object rawArgs = map.get("args");
            if (rawArgs instanceof Map) {
                args = (Map<String, String>) rawArgs;
            }
            aggConfig = new AggConfig(function, args);
        }
    }
}
