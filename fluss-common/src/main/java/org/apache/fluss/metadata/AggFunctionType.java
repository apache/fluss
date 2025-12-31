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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Collections;
import java.util.Locale;
import java.util.Set;

/**
 * Aggregation function type for aggregate merge engine.
 *
 * <p>This enum represents all supported aggregation function types that can be applied to
 * non-primary key columns in aggregation merge engine tables.
 */
@PublicEvolving
public enum AggFunctionType {
    // Numeric aggregation
    SUM,
    PRODUCT,
    MAX,
    MIN,

    // Value selection
    LAST_VALUE,
    LAST_VALUE_IGNORE_NULLS,
    FIRST_VALUE,
    FIRST_VALUE_IGNORE_NULLS,

    // String aggregation
    LISTAGG,
    STRING_AGG, // Alias for LISTAGG - maps to same factory

    // Boolean aggregation
    BOOL_AND,
    BOOL_OR;

    /** Parameter name for delimiter used in LISTAGG and STRING_AGG functions. */
    public static final String PARAM_DELIMITER = "delimiter";

    /**
     * Returns the set of supported parameter names for this aggregation function.
     *
     * @return an immutable set of parameter names
     */
    public Set<String> getSupportedParameters() {
        switch (this) {
            case LISTAGG:
            case STRING_AGG:
                // LISTAGG and STRING_AGG support optional "delimiter" parameter
                return Collections.singleton(PARAM_DELIMITER);
            default:
                // All other functions do not accept parameters
                return Collections.emptySet();
        }
    }

    /**
     * Validates a parameter value for this aggregation function.
     *
     * @param parameterName the parameter name
     * @param parameterValue the parameter value
     * @throws IllegalArgumentException if the parameter value is invalid
     */
    public void validateParameter(String parameterName, String parameterValue) {
        // Check if parameter is supported
        if (!getSupportedParameters().contains(parameterName)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Parameter '%s' is not supported for aggregation function '%s'. "
                                    + "Supported parameters: %s",
                            parameterName,
                            this,
                            getSupportedParameters().isEmpty()
                                    ? "none"
                                    : getSupportedParameters()));
        }

        // Validate parameter value based on function type and parameter name
        switch (this) {
            case LISTAGG:
            case STRING_AGG:
                if (PARAM_DELIMITER.equals(parameterName)) {
                    if (parameterValue == null || parameterValue.isEmpty()) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Parameter '%s' for aggregation function '%s' must be a non-empty string",
                                        parameterName, this));
                    }
                }
                break;
            default:
                // No validation needed for other functions (they don't have parameters)
                break;
        }
    }

    /**
     * Converts a string to an AggFunctionType enum value.
     *
     * <p>This method supports multiple naming formats:
     *
     * <ul>
     *   <li>Underscore format: "last_value_ignore_nulls"
     *   <li>Hyphen format: "last-value-ignore-nulls"
     *   <li>Case insensitive matching
     * </ul>
     *
     * <p>Note: For string_agg, this will return STRING_AGG enum, but the server-side factory will
     * map it to the same implementation as listagg.
     *
     * @param name the aggregation function type name
     * @return the AggFunctionType enum value, or null if not found
     */
    public static AggFunctionType fromString(String name) {
        if (name == null || name.trim().isEmpty()) {
            return null;
        }

        // Normalize the input: convert hyphens to underscores and uppercase
        String normalized = name.replace('-', '_').toUpperCase(Locale.ROOT).trim();

        try {
            return AggFunctionType.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Converts this AggFunctionType to its string identifier.
     *
     * <p>The identifier is the lowercase name with underscores, e.g., "sum", "last_value".
     *
     * @return the identifier string
     */
    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
