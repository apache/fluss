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

package org.apache.fluss.server.storage;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;

/**
 * Validator for the data disk write-limit configuration.
 *
 * <p>This validator is registered on the CoordinatorServer so invalid dynamic updates are rejected
 * before they are persisted to ZooKeeper. The TabletServer performs the same validation through
 * {@link LocalDiskManager}.
 */
public class DiskWriteLimitConfigValidator implements ServerReconfigurable {

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        String validationError =
                getValidationError(
                        newConfig.get(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO),
                        newConfig.get(ConfigOptions.SERVER_DATA_DISK_WRITE_RECOVER_RATIO));
        if (validationError != null) {
            throw new ConfigException(validationError);
        }
    }

    @Override
    public void reconfigure(Configuration newConfig) {}

    static String getValidationError(double writeLimitRatio, double writeRecoverRatio) {
        if (writeRecoverRatio > 0.0
                && writeRecoverRatio < writeLimitRatio
                && writeLimitRatio <= 1.0) {
            return null;
        }
        return String.format(
                "Invalid disk write-limit configuration: %s must be within (0.0, %s), and %s "
                        + "must be no greater than 1.0; %s=%s, %s=%s",
                ConfigOptions.SERVER_DATA_DISK_WRITE_RECOVER_RATIO.key(),
                ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO.key(),
                ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO.key(),
                ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO.key(),
                writeLimitRatio,
                ConfigOptions.SERVER_DATA_DISK_WRITE_RECOVER_RATIO.key(),
                writeRecoverRatio);
    }
}
