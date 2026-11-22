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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Parameter validation shared by the built-in {@link ConfigProvider}s. */
final class ProviderParams {

    private ProviderParams() {}

    /** Rejects unknown parameter keys so a typo'd parameter fails instead of being ignored. */
    static void validateKeys(String identifier, Map<String, String> params, String... knownKeys) {
        Set<String> known = new HashSet<>(Arrays.asList(knownKeys));
        for (String key : params.keySet()) {
            if (!known.contains(key)) {
                throw new IllegalArgumentException(
                        "Unknown parameter '"
                                + key
                                + "' for the '"
                                + identifier
                                + "' config provider. Supported parameters: "
                                + known
                                + ".");
            }
        }
    }
}
