/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;

import java.time.Duration;

/** Validates dynamic historical lookup cache settings. */
final class HistoricalLookupCacheConfigValidator implements ServerReconfigurable {

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        double newMaxRatio =
                newConfig.get(
                        ConfigOptions.SERVER_HISTORICAL_PARTITION_LOOKUP_CACHE_MAX_DISK_RATIO);
        if (!(newMaxRatio > 0.0 && newMaxRatio <= 1.0)) {
            throw new ConfigException(
                    String.format(
                            "Invalid configuration for %s, it must be within (0.0, 1.0].",
                            ConfigOptions.SERVER_HISTORICAL_PARTITION_LOOKUP_CACHE_MAX_DISK_RATIO
                                    .key()));
        }

        Duration newExpiration =
                newConfig.get(
                        ConfigOptions
                                .SERVER_HISTORICAL_PARTITION_LOOKUPER_CACHE_EXPIRE_AFTER_ACCESS);
        if (newExpiration.toMillis() < 1) {
            throw new ConfigException(
                    String.format(
                            "Invalid configuration for %s, it must be greater than or equal to 1 ms.",
                            ConfigOptions
                                    .SERVER_HISTORICAL_PARTITION_LOOKUPER_CACHE_EXPIRE_AFTER_ACCESS
                                    .key()));
        }
    }

    @Override
    public void reconfigure(Configuration newConfig) {}
}
