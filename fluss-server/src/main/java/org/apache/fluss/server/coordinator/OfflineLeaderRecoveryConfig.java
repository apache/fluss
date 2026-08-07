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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dynamically reconfigurable policy for recovering buckets without an active leader. */
public final class OfflineLeaderRecoveryConfig implements ServerReconfigurable {

    private static final Logger LOG = LoggerFactory.getLogger(OfflineLeaderRecoveryConfig.class);

    private volatile int cleanRetryCount;

    /** Creates the recovery policy from the server configuration. */
    public OfflineLeaderRecoveryConfig(Configuration conf) {
        int configuredCleanRetryCount = getConfiguredCleanRetryCount(conf);
        validateCleanRetryCount(configuredCleanRetryCount);
        this.cleanRetryCount = configuredCleanRetryCount;
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        validateCleanRetryCount(getConfiguredCleanRetryCount(newConfig));
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        int newCleanRetryCount = getConfiguredCleanRetryCount(newConfig);
        int previousCleanRetryCount = cleanRetryCount;
        cleanRetryCount = newCleanRetryCount;
        if (previousCleanRetryCount != newCleanRetryCount) {
            LOG.info(
                    "[OfflineLeaderRecovery] Updated clean retry count from {} to {}. Existing "
                            + "per-bucket retry counts are preserved.",
                    previousCleanRetryCount,
                    newCleanRetryCount);
        }
    }

    /** Returns the number of clean retries before unclean election is allowed. */
    public int getCleanRetryCount() {
        return cleanRetryCount;
    }

    private static int getConfiguredCleanRetryCount(Configuration conf) {
        return conf.get(ConfigOptions.COORDINATOR_OFFLINE_LEADER_CLEAN_RETRY_COUNT);
    }

    private static void validateCleanRetryCount(int cleanRetryCount) {
        if (cleanRetryCount < -1) {
            throw new ConfigException(
                    ConfigOptions.COORDINATOR_OFFLINE_LEADER_CLEAN_RETRY_COUNT.key()
                            + " must be greater than or equal to -1.");
        }
    }
}
