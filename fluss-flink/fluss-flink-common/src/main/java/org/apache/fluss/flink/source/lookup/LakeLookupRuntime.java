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

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.DataLakeUtils;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.FixedSchemaDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Runtime for asynchronous point lookups against a lake table. */
final class LakeLookupRuntime implements LookupRuntime, Serializable {

    private static final long serialVersionUID = 1L;

    private final FlussLookupRuntime flussLookupRuntime;
    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final org.apache.fluss.types.RowType flussFullRowType;
    private final int[] primaryKeyIndexes;
    private final Map<String, String> tableOptions;
    private final Duration lookupTimeout;
    private final int executorThreads;
    private final int maxConcurrency;

    private LakeTableLookuper lakeTableLookuper;
    private FixedSchemaDecoder lakeValueDecoder;
    private KeyEncoder lakePrimaryKeyEncoder;
    private KeyEncoder lakeBucketKeyEncoder;
    private BucketingFunction bucketingFunction;
    private PartitionGetter partitionGetter;
    private ThreadPoolExecutor lookupExecutor;

    private short lakeSchemaId;
    private int numBuckets;

    LakeLookupRuntime(
            FlussLookupRuntime flussLookupRuntime,
            Configuration flussConfig,
            TablePath tablePath,
            org.apache.fluss.types.RowType flussFullRowType,
            int[] primaryKeyIndexes,
            Map<String, String> tableOptions,
            Duration lookupTimeout,
            int executorThreads,
            int maxConcurrency) {
        this.flussLookupRuntime = flussLookupRuntime;
        this.flussConfig = flussConfig;
        this.tablePath = tablePath;
        this.flussFullRowType = flussFullRowType;
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.tableOptions = tableOptions;
        this.lookupTimeout = lookupTimeout;
        this.executorThreads = executorThreads;
        this.maxConcurrency = maxConcurrency;
    }

    @Override
    public void open() {
        TableInfo resolvedTableInfo = flussLookupRuntime.getTableInfo();
        DataLakeFormat dataLakeFormat = validateAndGetDataLakeFormat(resolvedTableInfo);
        org.apache.fluss.types.RowType lookupRowType = flussFullRowType.project(primaryKeyIndexes);
        lakePrimaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        lookupRowType,
                        resolvedTableInfo.getPhysicalPrimaryKeys(),
                        resolvedTableInfo.getTableConfig(),
                        resolvedTableInfo.isDefaultBucketKey());
        lakeBucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        lookupRowType,
                        resolvedTableInfo.getBucketKeys(),
                        resolvedTableInfo.getTableConfig(),
                        resolvedTableInfo.isDefaultBucketKey(),
                        lakePrimaryKeyEncoder);
        bucketingFunction = BucketingFunction.of(dataLakeFormat);
        partitionGetter = new PartitionGetter(lookupRowType, resolvedTableInfo.getPartitionKeys());
        numBuckets = resolvedTableInfo.getNumBuckets();
        lakeSchemaId = (short) resolvedTableInfo.getSchemaId();
        lakeValueDecoder =
                new FixedSchemaDecoder(
                        resolvedTableInfo.getTableConfig().getKvFormat(),
                        resolvedTableInfo.getSchema());
        lakeTableLookuper = createLakeTableLookuper(dataLakeFormat, resolvedTableInfo);
        lookupExecutor = createLookupExecutor();
    }

    @Override
    public CompletableFuture<LookupResult> lookup(RowData normalizedKeyRow) {
        final LakeTableLookuper lookuper = lakeTableLookuper;
        final FixedSchemaDecoder valueDecoder = lakeValueDecoder;
        final ThreadPoolExecutor executor = lookupExecutor;
        final byte[] keyBytes;
        final LakeTableLookuper.LookupContext lookupContext;

        try {
            InternalRow lookupRow = new FlinkAsFlussRow(normalizedKeyRow);
            keyBytes = encodePrimaryKey(lookupRow);
            lookupContext = createLookupContext(lookupRow, keyBytes);
        } catch (Throwable t) {
            return FutureUtils.completedExceptionally(t);
        }

        CompletableFuture<LookupResult> future = new CompletableFuture<>();
        try {
            executor.execute(
                    () -> {
                        try {
                            byte[] value = lookuper.lookup(keyBytes, lookupContext);
                            InternalRow row =
                                    value == null
                                            ? null
                                            : valueDecoder.decode(MemorySegment.wrap(value));
                            future.complete(new LookupResult(row));
                        } catch (Throwable t) {
                            future.completeExceptionally(
                                    new RuntimeException(
                                            "Execution of lake fallback lookup failed: "
                                                    + t.getMessage(),
                                            t));
                        }
                    });
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(
                    new RuntimeException("Lake fallback lookup executor is overloaded.", e));
        }

        return FutureUtils.orTimeout(
                future,
                lookupTimeout.toMillis(),
                TimeUnit.MILLISECONDS,
                "Lake fallback lookup timed out after " + lookupTimeout);
    }

    private byte[] encodePrimaryKey(InternalRow lookupRow) {
        return lakePrimaryKeyEncoder.encodeKey(lookupRow);
    }

    private LakeTableLookuper.LookupContext createLookupContext(
            InternalRow lookupRow, byte[] keyBytes) {
        final KeyEncoder primaryKeyEncoder = lakePrimaryKeyEncoder;
        byte[] bucketKeyBytes =
                lakeBucketKeyEncoder == primaryKeyEncoder
                        ? keyBytes
                        : lakeBucketKeyEncoder.encodeKey(lookupRow);
        int bucketId = bucketingFunction.bucketing(bucketKeyBytes, numBuckets);
        ResolvedPartitionSpec partitionSpec = partitionGetter.getResolvedPartitionSpec(lookupRow);
        LakeTableLookuper.LookupContext lookupContext =
                new LakeTableLookuper.LookupContext(
                        partitionSpec, bucketId, lakeSchemaId, flussFullRowType);
        return lookupContext;
    }

    @Override
    public void close() throws Exception {
        if (lookupExecutor != null) {
            lookupExecutor.shutdownNow();
        }
        if (lakeTableLookuper != null) {
            lakeTableLookuper.close();
        }
    }

    private ThreadPoolExecutor createLookupExecutor() {
        int queueCapacity = maxConcurrency - executorThreads;
        BlockingQueue<Runnable> queue =
                queueCapacity == 0
                        ? new SynchronousQueue<>()
                        : new ArrayBlockingQueue<>(queueCapacity);
        return new ThreadPoolExecutor(
                executorThreads,
                executorThreads,
                0L,
                TimeUnit.MILLISECONDS,
                queue,
                new ExecutorThreadFactory("fluss-lake-fallback-lookup"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    private DataLakeFormat validateAndGetDataLakeFormat(TableInfo tableInfo) {
        DataLakeFormat dataLakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
        if (dataLakeFormat != DataLakeFormat.PAIMON) {
            throw new TableException(
                    "Hybrid lake lookup currently only supports Paimon, but table "
                            + tablePath
                            + " uses "
                            + dataLakeFormat
                            + ".");
        }
        return dataLakeFormat;
    }

    private LakeTableLookuper createLakeTableLookuper(
            DataLakeFormat dataLakeFormat, TableInfo tableInfo) {
        Configuration tableConfiguration = Configuration.fromMap(tableOptions);
        Map<String, String> lakeCatalogProperties =
                DataLakeUtils.extractLakeCatalogProperties(tableConfiguration);
        LakeStoragePlugin lakeStoragePlugin =
                LakeStoragePluginSetUp.fromDataLakeFormat(dataLakeFormat.toString(), null);
        LakeStorage lakeStorage =
                checkNotNull(lakeStoragePlugin, "Lake storage plugin must not be null.")
                        .createLakeStorage(Configuration.fromMap(lakeCatalogProperties));
        return checkNotNull(
                lakeStorage.createLakeTableLookuper(
                        tablePath,
                        new LakeStorage.LookuperContext(
                                flussConfig.get(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR),
                                tableInfo.getTableConfig())),
                "Lake table lookuper must not be null.");
    }
}
