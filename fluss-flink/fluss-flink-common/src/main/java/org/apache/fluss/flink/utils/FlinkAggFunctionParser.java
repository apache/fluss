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

package org.apache.fluss.flink.utils;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.metadata.AggFunctions;

import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Parser for aggregation function configuration from Flink table properties.
 *
 * <p>Configuration format (column-level only):
 *
 * <pre>
 * fields.&lt;column&gt;.agg = &lt;function_name&gt;
 * fields.&lt;column&gt;.&lt;function_name&gt;.&lt;param_name&gt; = &lt;param_value&gt;
 * </pre>
 *
 * <p>Examples:
 *
 * <pre>
 * 'fields.tags.agg' = 'listagg'
 * 'fields.tags.listagg.delimiter' = ';'
 * </pre>
 */
public class FlinkAggFunctionParser {

    private static final String AGG_PREFIX = "fields.";
    private static final String AGG_SUFFIX = ".agg";

    /**
     * Parse aggregation function for a column.
     *
     * <p>Returns empty if no aggregation function is configured for this column.
     */
    public static Optional<AggFunction> parseAggFunction(String columnName, Configuration options) {

        // Check column-level configuration: fields.<column>.agg
        String columnFuncKey = AGG_PREFIX + columnName + AGG_SUFFIX;
        if (!options.containsKey(columnFuncKey)) {
            return Optional.empty();
        }

        // convert to lower case for consistent parameter option key
        String funcName = options.getString(columnFuncKey, null).toLowerCase();
        AggFunctionType type = parseAggFunctionType(funcName, columnName);

        // Collect column-level parameters: fields.<column>.<function_name>.*
        Map<String, String> params =
                collectParameters(AGG_PREFIX + columnName + "." + funcName + ".", options);

        return Optional.of(createAggFunction(type, params, columnName));
    }

    /**
     * Format aggregation function to Flink table options.
     *
     * <p>Converts: AggFunction → Map&lt;String, String&gt;
     *
     * <p>Output format:
     *
     * <pre>
     * fields.&lt;column&gt;.agg = &lt;function_name&gt;
     * fields.&lt;column&gt;.&lt;function_name&gt;.&lt;param&gt; = &lt;value&gt;
     * </pre>
     */
    public static void formatAggFunctionToOptions(
            String columnName, AggFunction aggFunction, Map<String, String> options) {
        // Set function name
        String funcKey = AGG_PREFIX + columnName + AGG_SUFFIX;
        // funcName has already in lower case
        String funcName = aggFunction.getType().toString();
        options.put(funcKey, funcName);

        // Set parameters: fields.<column>.<function_name>.<param>
        for (Map.Entry<String, String> param : aggFunction.getParameters().entrySet()) {
            String paramKey = AGG_PREFIX + columnName + "." + funcName + "." + param.getKey();
            options.put(paramKey, param.getValue());
        }
    }

    /**
     * Parse aggregation function type from string.
     *
     * @throws IllegalArgumentException if function name is invalid
     */
    @VisibleForTesting
    static AggFunctionType parseAggFunctionType(@Nullable String funcName, String columnName) {
        if (funcName == null || funcName.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Empty aggregation function name for column '%s'", columnName));
        }

        AggFunctionType type = AggFunctionType.fromString(funcName.trim());
        if (type == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unknown aggregation function for column '%s': %s\n"
                                    + "Supported functions: %s",
                            columnName, funcName, getSupportedFunctionNames()));
        }

        return type;
    }

    /**
     * Get a comma-separated list of all supported aggregation function names.
     *
     * <p>This method dynamically generates the list from the AggFunctionType enum, ensuring that
     * the error message automatically includes any newly added functions without requiring manual
     * updates.
     *
     * @return comma-separated list of supported function names
     */
    private static String getSupportedFunctionNames() {
        return Arrays.stream(AggFunctionType.values())
                .map(AggFunctionType::toString)
                .sorted()
                .collect(Collectors.joining(", "));
    }

    /**
     * Collect parameters with given prefix.
     *
     * <p>For prefix "fields.tags.listagg.", collects:
     *
     * <ul>
     *   <li>fields.tags.listagg.delimiter → delimiter
     *   <li>fields.tags.listagg.xxx → xxx
     * </ul>
     */
    @VisibleForTesting
    static Map<String, String> collectParameters(String prefix, Configuration options) {
        Map<String, String> params = new HashMap<>();

        for (String key : options.keySet()) {
            if (key.startsWith(prefix)) {
                String paramName = key.substring(prefix.length());
                String paramValue = options.getString(key, null);
                if (paramValue != null) {
                    params.put(paramName, paramValue);
                }
            }
        }

        return params;
    }

    /**
     * Create AggFunction from type and parameters.
     *
     * <p>This method uses the generic factory method from {@link AggFunctions} to create
     * aggregation functions, making it independent of specific function implementations. When new
     * aggregation functions are added to the system, no changes are required here.
     *
     * @param type the aggregation function type
     * @param params the function parameters (may be empty)
     * @param columnName the column name (used for error messages)
     * @return the created aggregation function
     */
    private static AggFunction createAggFunction(
            AggFunctionType type, Map<String, String> params, String columnName) {
        // Use generic factory method to create aggregation function
        // This delegates parameter handling and validation to the underlying implementation
        AggFunction aggFunction =
                params.isEmpty() ? AggFunctions.of(type) : AggFunctions.of(type, params);

        // Validate all parameters to ensure they are supported and valid
        try {
            aggFunction.validate();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid aggregation function configuration for column '%s': %s",
                            columnName, e.getMessage()),
                    e);
        }

        return aggFunction;
    }
}
