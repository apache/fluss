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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.TableConfig;

import javax.annotation.Nullable;

/** Server-side table config that falls back to server configuration for runtime options. */
public final class ResolvedTableConfig extends TableConfig {

    private final Configuration tableConfig;
    private final Configuration serverConfig;

    /**
     * Creates a resolved table config.
     *
     * @param tableConfig the table properties configuration
     * @param serverConfig the server configuration to fall back to for runtime options
     */
    public ResolvedTableConfig(Configuration tableConfig, @Nullable Configuration serverConfig) {
        super(tableConfig);
        this.tableConfig = tableConfig;
        this.serverConfig = serverConfig == null ? new Configuration() : serverConfig;
    }

    /** Gets the log segment file size resolved from table properties or server configuration. */
    @Override
    public MemorySize getLogSegmentSize() {
        if (tableConfig.contains(ConfigOptions.LOG_SEGMENT_FILE_SIZE)) {
            return tableConfig.get(ConfigOptions.LOG_SEGMENT_FILE_SIZE);
        }
        return serverConfig.get(ConfigOptions.LOG_SEGMENT_FILE_SIZE);
    }
}
