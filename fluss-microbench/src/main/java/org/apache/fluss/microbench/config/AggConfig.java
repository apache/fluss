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
 * Aggregation configuration for a column.
 *
 * <p>Supports both simple form ({@code agg: SUM}) and expanded form ({@code agg: {function:
 * LISTAGG, args: {delimiter: "|"}}}).
 */
public class AggConfig {

    private String function;
    private Map<String, String> args;

    public AggConfig() {}

    public AggConfig(String function, Map<String, String> args) {
        this.function = function;
        this.args = args;
    }

    public String function() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public Map<String, String> args() {
        return args;
    }

    public void setArgs(Map<String, String> args) {
        this.args = args;
    }
}
