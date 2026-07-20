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

import org.apache.fluss.annotation.Internal;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * A {@link ConfigProvider} resolving <code>${env:VAR_NAME}</code> markers from environment
 * variables. The optional {@code allowlist.pattern} parameter (a regex the variable name must fully
 * match) restricts which variables may be read.
 */
@Internal
public class EnvVarConfigProvider implements ConfigProvider {

    public static final String IDENTIFIER = "env";
    private static final String ALLOWLIST_PATTERN = "allowlist.pattern";

    private Pattern allowlist;

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public void configure(Map<String, String> params) {
        ProviderParams.validateKeys(IDENTIFIER, params, ALLOWLIST_PATTERN);
        String pattern = params.get(ALLOWLIST_PATTERN);
        if (pattern != null) {
            allowlist = Pattern.compile(pattern);
        }
    }

    @Override
    public String resolve(String path, String key) {
        if (!path.isEmpty()) {
            throw new IllegalArgumentException(
                    "The 'env' config provider takes no path segment; use ${env:VAR_NAME}, got path '"
                            + path
                            + "'.");
        }
        if (allowlist != null && !allowlist.matcher(key).matches()) {
            throw new IllegalArgumentException(
                    "Environment variable '"
                            + key
                            + "' is not permitted by allowlist.pattern of the 'env' config provider.");
        }
        String value = System.getenv(key);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Environment variable '"
                            + key
                            + "' referenced by ${env:"
                            + key
                            + "} is not set.");
        }
        return value;
    }
}
