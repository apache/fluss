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
import org.apache.fluss.config.cluster.ConfigValidator;
import org.apache.fluss.exception.ConfigException;

import javax.annotation.Nullable;

/**
 * Stateless validator for {@link ConfigOptions#SERVER_DATA_DISK_WRITE_LIMIT_RATIO}.
 *
 * <p>This validator ensures the disk write-limit ratio stays within the valid range (0.1, 1.0]. The
 * lower bound of 0.1 (exclusive) guarantees the recover threshold (ratio - 0.10) remains positive,
 * ensuring the system can always recover from a write-locked state. It is registered on the
 * CoordinatorServer so that invalid values are rejected upfront during {@code AlterConfigs}, before
 * being persisted to ZooKeeper.
 *
 * <p>The same range check is also performed on the TabletServer side by {@link LocalDiskManager}
 * (via {@link org.apache.fluss.config.cluster.ServerReconfigurable#validate}), providing
 * defense-in-depth.
 */
public class DiskWriteLimitRatioValidator implements ConfigValidator<Double> {

    @Override
    public String configKey() {
        return ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO.key();
    }

    @Override
    public void validate(@Nullable Double oldValue, @Nullable Double newValue)
            throws ConfigException {
        if (newValue == null) {
            // Deletion (reset to default) is always valid.
            return;
        }
        if (newValue <= 0.1 || newValue > 1.0) {
            throw new ConfigException(
                    String.format(
                            "Invalid %s: must be within (0.1, 1.0], but was %s",
                            ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO.key(), newValue));
        }
    }
}
