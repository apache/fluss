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

package org.apache.fluss.server.config;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ConfigValidator;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** Process-local, monotonic activation gate for the Manifest V2 writer. */
@Internal
public final class RemoteManifestV2WriterGate
        implements ServerReconfigurable, ConfigValidator<Boolean> {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteManifestV2WriterGate.class);
    private static final String IRREVERSIBLE_MESSAGE =
            "Manifest V2 writer activation is irreversible; the configuration cannot be "
                    + "disabled or deleted after activation.";

    private volatile boolean enabled;

    public RemoteManifestV2WriterGate(Configuration configuration) {
        this.enabled = configuration.get(ConfigOptions.REMOTE_LOG_MANIFEST_V2_WRITER_ENABLED);
    }

    /** Returns whether this process may publish Manifest V2. */
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String configKey() {
        return ConfigOptions.REMOTE_LOG_MANIFEST_V2_WRITER_ENABLED.key();
    }

    @Override
    public void validate(@Nullable Boolean oldValue, @Nullable Boolean newValue)
            throws ConfigException {
        if (Boolean.TRUE.equals(oldValue) && !Boolean.TRUE.equals(newValue)) {
            throw new ConfigException(IRREVERSIBLE_MESSAGE);
        }
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        if (enabled && !newConfig.get(ConfigOptions.REMOTE_LOG_MANIFEST_V2_WRITER_ENABLED)) {
            throw new ConfigException(IRREVERSIBLE_MESSAGE);
        }
    }

    @Override
    public void reconfigure(Configuration newConfig) {
        boolean newValue = newConfig.get(ConfigOptions.REMOTE_LOG_MANIFEST_V2_WRITER_ENABLED);
        if (newValue) {
            if (!enabled) {
                LOG.info("Manifest V2 writer has been activated for this server process.");
            }
            enabled = true;
        } else if (enabled) {
            LOG.error(
                    "Ignoring an attempt to disable the Manifest V2 writer after activation. "
                            + "The process-local gate remains enabled.");
        }
    }
}
