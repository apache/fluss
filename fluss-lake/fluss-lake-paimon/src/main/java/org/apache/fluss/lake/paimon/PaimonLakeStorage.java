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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeTableLookupRuntime;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.lake.paimon.lookup.PaimonLakeTableLookuper;
import org.apache.fluss.lake.paimon.lookup.SharedLookupFileCache;
import org.apache.fluss.lake.paimon.source.PaimonLakeSource;
import org.apache.fluss.lake.paimon.source.PaimonSplit;
import org.apache.fluss.lake.paimon.tiering.PaimonCommittable;
import org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory;
import org.apache.fluss.lake.paimon.tiering.PaimonWriteResult;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.options.MemorySize;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Paimon implementation of {@link LakeStorage}. */
public class PaimonLakeStorage implements LakeStorage {

    protected final Configuration paimonConfig;

    public PaimonLakeStorage(Configuration configuration) {
        this.paimonConfig = configuration;
    }

    @Override
    public LakeTieringFactory<PaimonWriteResult, PaimonCommittable> createLakeTieringFactory() {
        return new PaimonLakeTieringFactory(paimonConfig);
    }

    @Override
    public PaimonLakeCatalog createLakeCatalog() {
        return new PaimonLakeCatalog(paimonConfig);
    }

    @Override
    public LakeSource<PaimonSplit> createLakeSource(TablePath tablePath) {
        return new PaimonLakeSource(paimonConfig, tablePath);
    }

    @Override
    public LakeTableLookupRuntime createLakeTableLookupRuntime(
            String ioTmpDir, long lookupCacheMaxDiskBytes) {
        return new PaimonLakeTableLookupRuntime(ioTmpDir, lookupCacheMaxDiskBytes);
    }

    /** Paimon lookup runtime sharing one I/O manager across table lookupers. */
    private static final class PaimonLakeTableLookupRuntime implements LakeTableLookupRuntime {
        private final IOManager ioManager;
        private final SharedLookupFileCache lookupFileCache;

        private PaimonLakeTableLookupRuntime(String ioTmpDir, long lookupCacheMaxDiskBytes) {
            checkArgument(
                    lookupCacheMaxDiskBytes > 0, "lookupCacheMaxDiskBytes must be greater than 0.");
            this.ioManager = IOManager.create(checkNotNull(ioTmpDir, "ioTmpDir must not be null."));
            // ponytail: one runtime-wide retention; add a server option if this needs tuning.
            this.lookupFileCache =
                    new SharedLookupFileCache(
                            CoreOptions.LOOKUP_CACHE_FILE_RETENTION.defaultValue(),
                            new MemorySize(lookupCacheMaxDiskBytes));
        }

        @Override
        public LakeTableLookuper createLakeTableLookuper(TablePath tablePath, Context context) {
            return new PaimonLakeTableLookuper(
                    new Configuration(context.lakeConfiguration()),
                    tablePath,
                    ioManager,
                    lookupFileCache,
                    context.cacheNamespace(),
                    context.tableConfig(),
                    context.diskWriteGuard());
        }

        @Override
        public void updateLookupCacheMaxDiskBytes(long lookupCacheMaxDiskBytes) {
            checkArgument(
                    lookupCacheMaxDiskBytes > 0, "lookupCacheMaxDiskBytes must be greater than 0.");
            lookupFileCache.updateMaxDiskSize(new MemorySize(lookupCacheMaxDiskBytes));
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(lookupFileCache, "shared Paimon lookup-file cache");
            IOUtils.closeQuietly(ioManager, "shared Paimon lookup IO manager");
        }
    }
}
