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

package org.apache.fluss.config.cluster;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.exception.ConfigException;

import javax.annotation.Nullable;

/**
 * Validator for a single dynamic configuration key.
 *
 * <p>Unlike {@link ServerReconfigurable}, validators are stateless and only perform validation
 * logic without requiring component instances. This allows coordinators to validate configurations
 * for components they don't run (e.g., KvManager).
 *
 * <p>Example use case: CoordinatorServer needs to validate KV-related configurations even though it
 * doesn't have a KvManager instance. A {@link ConfigValidator} can be registered on both
 * CoordinatorServer (for validation) and TabletServer (for both validation and actual
 * reconfiguration via {@link ServerReconfigurable}).
 *
 * <p>Each validator monitors a single configuration key. The validator will only be invoked when
 * that specific key changes, improving validation efficiency.
 *
 * <p>This interface is designed to be stateless and thread-safe. Implementations should not rely on
 * any mutable component state.
 *
 * <p><b>Type-safe validation:</b> The validator receives strongly-typed values that have already
 * been parsed and validated for basic type correctness. This avoids redundant string parsing and
 * allows validators to focus on business logic validation.
 *
 * @param <T> the type of the configuration value being validated
 */
@PublicEvolving
public interface ConfigValidator<T> {

    /**
     * Returns the configuration key this validator monitors.
     *
     * <p>The validator will only be invoked when this specific configuration key changes. This
     * allows efficient filtering of validators and clear declaration of dependencies.
     *
     * @return the configuration key to monitor, must not be null or empty
     */
    String configKey();

    /**
     * Validates a configuration value change.
     *
     * <p>This method is called when the monitored configuration key changes. It should check
     * whether the new value is valid, potentially considering the old value and validation rules.
     *
     * <p>The method should be stateless and deterministic - given the same old and new values, it
     * should always produce the same validation result.
     *
     * @param oldValue the previous value of the configuration key, null if the key was not set
     *     before
     * @param newValue the new value of the configuration key, null if the key is being deleted
     * @throws ConfigException if the configuration change is invalid, with a descriptive error
     *     message explaining why the change cannot be applied
     */
    void validate(@Nullable T oldValue, @Nullable T newValue) throws ConfigException;
}
