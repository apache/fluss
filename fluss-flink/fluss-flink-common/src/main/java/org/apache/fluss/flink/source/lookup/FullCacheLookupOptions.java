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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.flink.FlinkConnectorOptions;

import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Validated resource options for a bucket-sharded full lookup cache. */
final class FullCacheLookupOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    final long writeBufferSize;
    final int writeBufferCount;
    final long blockCacheSize;
    final int maxBackgroundJobs;
    final int maxOpenFiles;

    FullCacheLookupOptions(ReadableConfig config) {
        writeBufferSize =
                config.get(FlinkConnectorOptions.LOOKUP_FULL_CACHE_WRITE_BUFFER_SIZE).getBytes();
        writeBufferCount = config.get(FlinkConnectorOptions.LOOKUP_FULL_CACHE_WRITE_BUFFER_COUNT);
        blockCacheSize =
                config.get(FlinkConnectorOptions.LOOKUP_FULL_CACHE_BLOCK_CACHE_SIZE).getBytes();
        maxBackgroundJobs = config.get(FlinkConnectorOptions.LOOKUP_FULL_CACHE_MAX_BACKGROUND_JOBS);
        maxOpenFiles = config.get(FlinkConnectorOptions.LOOKUP_FULL_CACHE_MAX_OPEN_FILES);
        checkArgument(
                writeBufferSize > 0,
                "'%s' must be positive.",
                FlinkConnectorOptions.LOOKUP_FULL_CACHE_WRITE_BUFFER_SIZE.key());
        checkArgument(
                writeBufferCount >= 2,
                "'%s' must be at least 2.",
                FlinkConnectorOptions.LOOKUP_FULL_CACHE_WRITE_BUFFER_COUNT.key());
        checkArgument(
                blockCacheSize > 0,
                "'%s' must be positive.",
                FlinkConnectorOptions.LOOKUP_FULL_CACHE_BLOCK_CACHE_SIZE.key());
        checkArgument(
                maxBackgroundJobs > 0,
                "'%s' must be positive.",
                FlinkConnectorOptions.LOOKUP_FULL_CACHE_MAX_BACKGROUND_JOBS.key());
        checkArgument(
                maxOpenFiles > 0,
                "'%s' must be positive.",
                FlinkConnectorOptions.LOOKUP_FULL_CACHE_MAX_OPEN_FILES.key());
    }
}
