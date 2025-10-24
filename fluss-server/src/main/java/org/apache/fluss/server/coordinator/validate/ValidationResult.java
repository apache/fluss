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

package org.apache.fluss.server.coordinator.validate;

import org.apache.fluss.annotation.Internal;

import java.util.ArrayList;
import java.util.List;

/**
 * Validation result for configuration validation operations. This class contains validation status,
 * errors, and warnings.
 */
@Internal
public class ValidationResult {
    private final boolean valid;
    private final List<String> errors;
    private final List<String> warnings;

    public ValidationResult(boolean valid, List<String> errors, List<String> warnings) {
        this.valid = valid;
        this.errors = errors != null ? errors : new ArrayList<>();
        this.warnings = warnings != null ? warnings : new ArrayList<>();
    }

    public boolean isValid() {
        return valid;
    }

    public List<String> getErrors() {
        return errors;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public boolean hasWarnings() {
        return !warnings.isEmpty();
    }

    public static ValidationResult success() {
        return new ValidationResult(true, null, null);
    }

    public static ValidationResult success(List<String> warnings) {
        return new ValidationResult(true, null, warnings);
    }

    public static ValidationResult failure(List<String> errors) {
        return new ValidationResult(false, errors, null);
    }

    public static ValidationResult failure(List<String> errors, List<String> warnings) {
        return new ValidationResult(false, errors, warnings);
    }

    public static ValidationResult failure(String error) {
        List<String> errors = new ArrayList<>();
        errors.add(error);
        return new ValidationResult(false, errors, null);
    }

    /**
     * Merges this validation result with another one.
     *
     * @param other the other validation result to merge
     * @return a new validation result containing errors and warnings from both results
     */
    public ValidationResult merge(ValidationResult other) {
        List<String> mergedErrors = new ArrayList<>(this.errors);
        mergedErrors.addAll(other.errors);

        List<String> mergedWarnings = new ArrayList<>(this.warnings);
        mergedWarnings.addAll(other.warnings);

        boolean isValid = this.valid && other.valid;
        return new ValidationResult(isValid, mergedErrors, mergedWarnings);
    }
}
