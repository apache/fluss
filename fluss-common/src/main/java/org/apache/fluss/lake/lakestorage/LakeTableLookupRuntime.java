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

package org.apache.fluss.lake.lakestorage;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.TablePath;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** TabletServer-scoped runtime managing shared resources and creating lake table lookupers. */
@PublicEvolving
public interface LakeTableLookupRuntime extends AutoCloseable {

    /**
     * Creates a table-level point lookuper for the specified lake table.
     *
     * @param tablePath the logical path identifying the table in the lakehouse storage
     * @param context runtime context for creating the lookuper
     * @return a table-level point lookuper
     */
    LakeTableLookuper createLakeTableLookuper(TablePath tablePath, Context context);

    /**
     * Applies a new snapshot of the runtime resource settings to existing and future lookupers
     * without replacing them.
     *
     * <p>This method may be called concurrently with lookuper creation and lookups. Implementations
     * must apply the settings in a thread-safe manner. Lake-format and table-specific configuration
     * is outside the scope of this method.
     *
     * @param options the new runtime resource settings
     */
    void reconfigure(LookupRuntimeOptions options);

    /**
     * Immutable snapshot of the format-independent resource settings for a lookup runtime.
     *
     * <p>These settings apply to resources shared by all table lookupers in one runtime.
     * Lake-format and table-specific configuration is supplied separately when creating a lookuper.
     */
    final class LookupRuntimeOptions {

        private final long localCacheMaxBytes;

        /**
         * Creates runtime resource settings.
         *
         * @param localCacheMaxBytes positive disk-space budget in bytes for local caches shared by
         *     all lookupers in the runtime; implementations without local disk caches may ignore
         *     this budget
         */
        public LookupRuntimeOptions(long localCacheMaxBytes) {
            checkArgument(localCacheMaxBytes > 0, "localCacheMaxBytes must be greater than 0.");
            this.localCacheMaxBytes = localCacheMaxBytes;
        }

        /** Returns the runtime-wide disk-space budget for local caches, in bytes. */
        public long localCacheMaxBytes() {
            return localCacheMaxBytes;
        }
    }

    /** Runtime context for creating a lake table lookuper. */
    final class Context {
        private final Configuration lakeConfiguration;
        private final String cacheNamespace;
        private final TableConfig tableConfig;
        private final Runnable diskWriteGuard;

        /**
         * Creates a lookuper context.
         *
         * @param lakeConfiguration configuration of the lake storage for this lookuper
         * @param cacheNamespace namespace identifying cache entries owned by this lookuper
         * @param tableConfig configuration of the Fluss table
         * @param diskWriteGuard guard invoked before creating a local lookup cache file
         */
        public Context(
                Configuration lakeConfiguration,
                String cacheNamespace,
                TableConfig tableConfig,
                Runnable diskWriteGuard) {
            this.lakeConfiguration =
                    checkNotNull(lakeConfiguration, "lakeConfiguration must not be null.");
            this.cacheNamespace = checkNotNull(cacheNamespace, "cacheNamespace must not be null.");
            this.tableConfig = checkNotNull(tableConfig, "tableConfig must not be null.");
            this.diskWriteGuard = checkNotNull(diskWriteGuard, "diskWriteGuard must not be null.");
        }

        /** Returns the lake storage configuration for this lookuper. */
        public Configuration lakeConfiguration() {
            return lakeConfiguration;
        }

        /** Returns the namespace identifying cache entries owned by this lookuper. */
        public String cacheNamespace() {
            return cacheNamespace;
        }

        /** Returns the configuration of the Fluss table. */
        public TableConfig tableConfig() {
            return tableConfig;
        }

        /** Returns the guard invoked before creating a local lookup cache file. */
        public Runnable diskWriteGuard() {
            return diskWriteGuard;
        }
    }
}
