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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry for configuration validators. This class manages the registration and retrieval of
 * validators used during table DDL operations.
 */
@Internal
public class ValidatorRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(ValidatorRegistry.class);

    private final List<ConfigValidator> validators = new CopyOnWriteArrayList<>();
    private volatile List<ConfigValidator> sortedValidators = new ArrayList<>();

    /**
     * Registers a configuration validator.
     *
     * @param validator the validator to register
     */
    public void register(ConfigValidator validator) {
        if (validator == null) {
            throw new IllegalArgumentException("Validator cannot be null");
        }

        validators.add(validator);
        updateSortedValidators();
        LOG.info("Registered configuration validator: {}", validator.getName());
    }

    /**
     * Unregisters a configuration validator.
     *
     * @param validator the validator to unregister
     * @return true if the validator was found and removed
     */
    public boolean unregister(ConfigValidator validator) {
        if (validator == null) {
            return false;
        }

        boolean removed = validators.remove(validator);
        if (removed) {
            updateSortedValidators();
            LOG.info("Unregistered configuration validator: {}", validator.getName());
        }
        return removed;
    }

    /**
     * Gets all registered validators in priority order.
     *
     * @return list of validators sorted by priority
     */
    public List<ConfigValidator> getAllValidators() {
        return Collections.unmodifiableList(sortedValidators);
    }

    /**
     * Gets validators that handle the specified configuration key.
     *
     * @param key the configuration key
     * @return list of validators that handle this key, sorted by priority
     */
    public List<ConfigValidator> getValidatorsForKey(String key) {
        List<ConfigValidator> result = new ArrayList<>();
        for (ConfigValidator validator : sortedValidators) {
            if (validator.handles(key)) {
                result.add(validator);
            }
        }
        return result;
    }

    /**
     * Gets the number of registered validators.
     *
     * @return the number of registered validators
     */
    public int size() {
        return validators.size();
    }

    /** Clears all registered validators. */
    public void clear() {
        validators.clear();
        updateSortedValidators();
        LOG.info("Cleared all configuration validators");
    }

    /** Updates the sorted validators list based on priority. */
    private void updateSortedValidators() {
        List<ConfigValidator> newSorted = new ArrayList<>(validators);
        newSorted.sort(Comparator.comparingInt(ConfigValidator::getPriority));
        sortedValidators = newSorted;
    }
}
