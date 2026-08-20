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

package org.apache.fluss.lake.paimon.lookup;

import org.apache.fluss.lake.paimon.utils.PaimonPartitionBucket;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Adapts Paimon's {@link LocalTableQuery} to Fluss's long-lived historical lookup lifecycle.
 *
 * <p>{@code LocalTableQuery} does not discover source files by itself. Fluss must register the
 * files for each partition-bucket before its first lookup and refresh that registration when
 * compaction and snapshot expiration make a cached file stale. This class owns that file state and
 * retries a failed lookup once, while ensuring that concurrent failures trigger only one refresh.
 *
 * <p>It also isolates synchronization differences between supported Paimon versions. Paimon 1.3
 * synchronizes lookup on the {@code LocalTableQuery} instance, so file registration must use the
 * same monitor. Paimon 2.0 supports concurrent lookup and coordinates access by bucket, allowing
 * registration to use a per-bucket lock instead. Keeping these concerns here lets {@link
 * PaimonLakeTableLookuper} focus on Fluss key, partition, and value conversion.
 */
class PaimonLocalTableQuery implements Closeable {

    private static final boolean CONCURRENT_LOOKUP_SUPPORTED = supportsConcurrentLookup();

    private final FileStoreTable fileStoreTable;
    private final LocalTableQuery localTableQuery;
    private final Map<PaimonPartitionBucket, BucketState> bucketStates;

    public PaimonLocalTableQuery(FileStoreTable fileStoreTable, LocalTableQuery localTableQuery) {
        this.fileStoreTable = checkNotNull(fileStoreTable, "fileStoreTable must not be null.");
        this.localTableQuery = checkNotNull(localTableQuery, "localTableQuery must not be null.");
        this.bucketStates = new ConcurrentHashMap<>();
    }

    final @Nullable InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
            throws IOException {
        List<DataFileMeta> filesBeforeLookup = initializeFiles(partition, bucket);
        try {
            return localTableQuery.lookup(partition, bucket, key);
        } catch (IOException firstError) {
            refreshFiles(partition, bucket, filesBeforeLookup);
            try {
                return localTableQuery.lookup(partition, bucket, key);
            } catch (IOException retryError) {
                retryError.addSuppressed(firstError);
                throw retryError;
            }
        }
    }

    private List<DataFileMeta> initializeFiles(BinaryRow partition, int bucket) {
        PaimonPartitionBucket partitionBucket = new PaimonPartitionBucket(partition, bucket);
        BucketState bucketState =
                bucketStates.computeIfAbsent(partitionBucket, ignored -> new BucketState());
        List<DataFileMeta> files = bucketState.files;
        if (files != null) {
            return files;
        }

        Object lockScope = fileRegistrationLock(bucketState);
        synchronized (lockScope) {
            files = bucketState.files;
            if (files == null) {
                files = registerFiles(partition, bucket, Collections.emptyList());
                bucketState.files = files;
            }
            return files;
        }
    }

    private void refreshFiles(
            BinaryRow partition, int bucket, List<DataFileMeta> filesBeforeLookup) {
        PaimonPartitionBucket partitionBucket = new PaimonPartitionBucket(partition, bucket);
        BucketState bucketState =
                checkNotNull(
                        bucketStates.get(partitionBucket),
                        "Partition-bucket files must be initialized.");
        if (bucketState.files != filesBeforeLookup) {
            return;
        }

        Object lockScope = fileRegistrationLock(bucketState);
        synchronized (lockScope) {
            if (bucketState.files != filesBeforeLookup) {
                return;
            }
            bucketState.files = registerFiles(partition, bucket, filesBeforeLookup);
        }
    }

    Object fileRegistrationLock(BucketState bucketState) {
        if (CONCURRENT_LOOKUP_SUPPORTED) {
            return bucketState;
        }
        return localTableQuery;
    }

    @Override
    public final void close() throws IOException {
        try {
            localTableQuery.close();
        } finally {
            bucketStates.clear();
        }
    }

    private List<DataFileMeta> registerFiles(
            BinaryRow partition, int bucket, List<DataFileMeta> filesBeforeRefresh) {
        List<DataFileMeta> latestFiles = scanDataFiles(partition, bucket);
        localTableQuery.refreshFiles(partition, bucket, filesBeforeRefresh, latestFiles);
        return latestFiles;
    }

    final List<DataFileMeta> scanDataFiles(BinaryRow partition, int bucket) {
        LinkedHashMap<String, DataFileMeta> dataFilesByName = new LinkedHashMap<>();
        InnerTableScan tableScan =
                fileStoreTable
                        .newScan()
                        .withPartitionFilter(Collections.singletonList(partition))
                        .withBucket(bucket);
        for (Split split : tableScan.plan().splits()) {
            if (split instanceof DataSplit) {
                for (DataFileMeta file : ((DataSplit) split).dataFiles()) {
                    dataFilesByName.put(file.fileName(), file);
                }
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(dataFilesByName.values()));
    }

    private static boolean supportsConcurrentLookup() {
        try {
            Method lookupMethod =
                    LocalTableQuery.class.getMethod(
                            "lookup", BinaryRow.class, int.class, InternalRow.class);
            return !Modifier.isSynchronized(lookupMethod.getModifiers());
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                    "Unsupported Paimon LocalTableQuery lookup signature.", e);
        }
    }

    private static final class BucketState {
        // Mirrors the file membership retained by LocalTableQuery. Disk lookup-file eviction does
        // not remove that query state, so this list remains the next refreshFiles' beforeFiles.
        private volatile @Nullable List<DataFileMeta> files;
    }
}
