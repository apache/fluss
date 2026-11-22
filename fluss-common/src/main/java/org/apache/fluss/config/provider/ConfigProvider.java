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

package org.apache.fluss.config.provider;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Map;

/**
 * Resolves indirect configuration values, referenced as <code>${identifier:[path:]key}</code>
 * markers, from an external source such as environment variables or files.
 *
 * <p>Providers are discovered via {@link java.util.ServiceLoader} and are only active when listed
 * in {@code config.providers}. Implementations must be thread-safe.
 *
 * @since 1.0
 */
@PublicEvolving
public interface ConfigProvider extends AutoCloseable {

    /** The name referencing this provider in markers, e.g. {@code "env"}. Must be unique. */
    String identifier();

    /** Applies {@code config.providers.<identifier>.param.*} parameters, prefix stripped. */
    default void configure(Map<String, String> params) {}

    /**
     * Resolves the value for a single marker.
     *
     * @param path the middle segment of the marker, or an empty string when absent
     * @param key the final segment of the marker
     * @return the resolved value, never {@code null}
     * @throws IllegalArgumentException if the value cannot be resolved
     */
    String resolve(String path, String key);

    @Override
    default void close() {}
}
