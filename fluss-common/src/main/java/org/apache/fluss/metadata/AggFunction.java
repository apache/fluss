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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregation function with optional parameters for aggregate merge engine.
 *
 * <p>This class represents a parameterized aggregation function that can be applied to non-primary
 * key columns in aggregation merge engine tables. It encapsulates both the function type and
 * function-specific parameters (e.g., delimiter for LISTAGG).
 *
 * <p>Use {@link AggFunctions} utility class to create instances:
 *
 * <pre>{@code
 * AggFunction sumFunc = AggFunctions.SUM();
 * AggFunction listaggFunc = AggFunctions.LISTAGG(";");
 * }</pre>
 *
 * @since 0.9
 */
@PublicEvolving
public final class AggFunction implements Serializable {

    private static final long serialVersionUID = 1L;

    private final AggFunctionType type;
    private final Map<String, String> parameters;

    /**
     * Creates an aggregation function with the specified type and parameters.
     *
     * @param type the aggregation function type
     * @param parameters the function parameters (nullable)
     */
    AggFunction(AggFunctionType type, @Nullable Map<String, String> parameters) {
        this.type = Objects.requireNonNull(type, "Aggregation function type must not be null");
        this.parameters =
                parameters == null || parameters.isEmpty()
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(new HashMap<>(parameters));
    }

    /**
     * Returns the aggregation function type.
     *
     * @return the function type
     */
    public AggFunctionType getType() {
        return type;
    }

    /**
     * Returns the function parameters.
     *
     * @return an immutable map of parameters
     */
    public Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * Gets a specific parameter value.
     *
     * @param key the parameter key
     * @return the parameter value, or null if not found
     */
    @Nullable
    public String getParameter(String key) {
        return parameters.get(key);
    }

    /**
     * Checks if this function has any parameters.
     *
     * @return true if parameters are present, false otherwise
     */
    public boolean hasParameters() {
        return !parameters.isEmpty();
    }

    /**
     * Validates all parameters of this aggregation function.
     *
     * <p>This method checks that:
     *
     * <ul>
     *   <li>All parameter names are supported by the function type
     *   <li>All parameter values are valid
     * </ul>
     *
     * @throws IllegalArgumentException if any parameter is invalid
     */
    public void validate() {
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            type.validateParameter(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AggFunction that = (AggFunction) o;
        return type == that.type && parameters.equals(that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, parameters);
    }

    @Override
    public String toString() {
        if (parameters.isEmpty()) {
            return type.toString();
        }
        return type.toString() + parameters;
    }
}
