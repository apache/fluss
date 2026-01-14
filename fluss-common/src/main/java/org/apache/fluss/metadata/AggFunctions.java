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

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for creating parameterized aggregation functions.
 *
 * <p>This class provides factory methods for all supported aggregation functions with their
 * respective parameters. Use these methods when defining aggregate columns in a schema.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.BIGINT())
 *     .column("count", DataTypes.BIGINT(), AggFunctions.SUM())
 *     .column("tags", DataTypes.STRING(), AggFunctions.LISTAGG(";"))
 *     .primaryKey("id")
 *     .build();
 * }</pre>
 *
 * @since 0.9
 */
@PublicEvolving
// CHECKSTYLE.OFF: MethodName - Factory methods use uppercase naming convention like DataTypes
public final class AggFunctions {

    private AggFunctions() {
        // Utility class, no instantiation
    }

    // ===================================================================================
    // Numeric Aggregation Functions
    // ===================================================================================

    /**
     * Creates a SUM aggregation function that computes the sum of numeric values.
     *
     * <p>Supported data types: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL
     *
     * @return a SUM aggregation function
     */
    public static AggFunction SUM() {
        return new AggFunction(AggFunctionType.SUM, null);
    }

    /**
     * Creates a PRODUCT aggregation function that computes the product of numeric values.
     *
     * <p>Supported data types: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL
     *
     * @return a PRODUCT aggregation function
     */
    public static AggFunction PRODUCT() {
        return new AggFunction(AggFunctionType.PRODUCT, null);
    }

    /**
     * Creates a MAX aggregation function that selects the maximum value.
     *
     * <p>Supported data types: CHAR, STRING, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE,
     * DECIMAL, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ
     *
     * @return a MAX aggregation function
     */
    public static AggFunction MAX() {
        return new AggFunction(AggFunctionType.MAX, null);
    }

    /**
     * Creates a MIN aggregation function that selects the minimum value.
     *
     * <p>Supported data types: CHAR, STRING, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE,
     * DECIMAL, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ
     *
     * @return a MIN aggregation function
     */
    public static AggFunction MIN() {
        return new AggFunction(AggFunctionType.MIN, null);
    }

    // ===================================================================================
    // Value Selection Functions
    // ===================================================================================

    /**
     * Creates a LAST_VALUE aggregation function that replaces the previous value with the most
     * recently received value.
     *
     * <p>Supported data types: All data types
     *
     * <p>Null handling: Null values will overwrite previous values
     *
     * @return a LAST_VALUE aggregation function
     */
    public static AggFunction LAST_VALUE() {
        return new AggFunction(AggFunctionType.LAST_VALUE, null);
    }

    /**
     * Creates a LAST_VALUE_IGNORE_NULLS aggregation function that replaces the previous value with
     * the latest non-null value.
     *
     * <p>This is the default aggregate function when no function is specified.
     *
     * <p>Supported data types: All data types
     *
     * <p>Null handling: Null values are ignored, previous value is retained
     *
     * @return a LAST_VALUE_IGNORE_NULLS aggregation function
     */
    public static AggFunction LAST_VALUE_IGNORE_NULLS() {
        return new AggFunction(AggFunctionType.LAST_VALUE_IGNORE_NULLS, null);
    }

    /**
     * Creates a FIRST_VALUE aggregation function that retains the first value seen for a field.
     *
     * <p>Supported data types: All data types
     *
     * <p>Null handling: Null values are retained if received first
     *
     * @return a FIRST_VALUE aggregation function
     */
    public static AggFunction FIRST_VALUE() {
        return new AggFunction(AggFunctionType.FIRST_VALUE, null);
    }

    /**
     * Creates a FIRST_VALUE_IGNORE_NULLS aggregation function that selects the first non-null value
     * in a data set.
     *
     * <p>Supported data types: All data types
     *
     * <p>Null handling: Null values are ignored until a non-null value is received
     *
     * @return a FIRST_VALUE_IGNORE_NULLS aggregation function
     */
    public static AggFunction FIRST_VALUE_IGNORE_NULLS() {
        return new AggFunction(AggFunctionType.FIRST_VALUE_IGNORE_NULLS, null);
    }

    // ===================================================================================
    // String Aggregation Functions
    // ===================================================================================

    /**
     * Default delimiter for LISTAGG and STRING_AGG aggregation functions.
     *
     * @since 0.9
     */
    public static final String DEFAULT_LISTAGG_DELIMITER = ",";

    /**
     * Creates a LISTAGG aggregation function with default comma delimiter.
     *
     * <p>Concatenates multiple string values into a single string with a delimiter.
     *
     * <p>Supported data types: STRING, CHAR
     *
     * <p>Null handling: Null values are skipped
     *
     * @return a LISTAGG aggregation function with default delimiter
     */
    public static AggFunction LISTAGG() {
        return new AggFunction(AggFunctionType.LISTAGG, null);
    }

    /**
     * Creates a LISTAGG aggregation function with the specified delimiter.
     *
     * <p>Concatenates multiple string values into a single string with the specified delimiter.
     *
     * <p>Supported data types: STRING, CHAR
     *
     * <p>Null handling: Null values are skipped
     *
     * @param delimiter the delimiter to use for concatenation
     * @return a LISTAGG aggregation function with the specified delimiter
     */
    public static AggFunction LISTAGG(String delimiter) {
        Map<String, String> params = new HashMap<>();
        params.put("delimiter", delimiter);
        return new AggFunction(AggFunctionType.LISTAGG, params);
    }

    /**
     * Creates a STRING_AGG aggregation function with default comma delimiter.
     *
     * <p>Alias for {@link #LISTAGG()}. Concatenates multiple string values into a single string
     * with a delimiter.
     *
     * <p>Supported data types: STRING, CHAR
     *
     * <p>Null handling: Null values are skipped
     *
     * @return a STRING_AGG aggregation function with default delimiter
     */
    public static AggFunction STRING_AGG() {
        return new AggFunction(AggFunctionType.STRING_AGG, null);
    }

    /**
     * Creates a STRING_AGG aggregation function with the specified delimiter.
     *
     * <p>Alias for {@link #LISTAGG(String)}. Concatenates multiple string values into a single
     * string with the specified delimiter.
     *
     * <p>Supported data types: STRING, CHAR
     *
     * <p>Null handling: Null values are skipped
     *
     * @param delimiter the delimiter to use for concatenation
     * @return a STRING_AGG aggregation function with the specified delimiter
     */
    public static AggFunction STRING_AGG(String delimiter) {
        Map<String, String> params = new HashMap<>();
        params.put("delimiter", delimiter);
        return new AggFunction(AggFunctionType.STRING_AGG, params);
    }

    // ===================================================================================
    // Boolean Aggregation Functions
    // ===================================================================================

    /**
     * Creates a BOOL_AND aggregation function that evaluates whether all boolean values in a set
     * are true (logical AND).
     *
     * <p>Supported data types: BOOLEAN
     *
     * <p>Null handling: Null values are ignored
     *
     * @return a BOOL_AND aggregation function
     */
    public static AggFunction BOOL_AND() {
        return new AggFunction(AggFunctionType.BOOL_AND, null);
    }

    /**
     * Creates a BOOL_OR aggregation function that checks if at least one boolean value in a set is
     * true (logical OR).
     *
     * <p>Supported data types: BOOLEAN
     *
     * <p>Null handling: Null values are ignored
     *
     * @return a BOOL_OR aggregation function
     */
    public static AggFunction BOOL_OR() {
        return new AggFunction(AggFunctionType.BOOL_OR, null);
    }

    // ===================================================================================
    // Internal Factory Methods
    // ===================================================================================

    /**
     * Creates an aggregation function from a type and parameters map. Used internally for
     * deserialization.
     *
     * @param type the aggregation function type
     * @param parameters the function parameters
     * @return an aggregation function
     */
    public static AggFunction of(AggFunctionType type, Map<String, String> parameters) {
        return new AggFunction(type, parameters);
    }

    /**
     * Creates an aggregation function from a type. Used internally for deserialization.
     *
     * @param type the aggregation function type
     * @return an aggregation function
     */
    public static AggFunction of(AggFunctionType type) {
        return new AggFunction(type, null);
    }
}
// CHECKSTYLE.ON: MethodName
