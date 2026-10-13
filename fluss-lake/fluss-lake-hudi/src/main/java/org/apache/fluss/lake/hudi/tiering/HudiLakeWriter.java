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

package org.apache.fluss.lake.hudi.tiering;

import org.apache.fluss.lake.hudi.tiering.writer.HudiRecordWriter;
import org.apache.fluss.lake.hudi.utils.meta.CkpMetadata;
import org.apache.fluss.lake.hudi.utils.meta.CkpMetadataProvider;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.TimeUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.lake.writer.WriterInitContext.UNKNOWN_SPLIT_INDEX;
import static org.apache.fluss.lake.writer.WriterInitContext.UNKNOWN_TIERING_ROUND_TIMESTAMP;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Hudi implementation of {@link LakeWriter}. */
public class HudiLakeWriter implements LakeWriter<HudiWriteResult> {

    private static final Logger LOG = LoggerFactory.getLogger(HudiLakeWriter.class);

    private static final String COMPACTION_COMPLETE_TIMEOUT_KEY =
            "fluss.tiering.compaction.complete-timeout";
    private static final String COMPACTION_SHUTDOWN_TIMEOUT_KEY =
            "fluss.tiering.compaction.shutdown-timeout";
    private static final Duration DEFAULT_COMPACTION_COMPLETE_TIMEOUT = Duration.ofMinutes(30);
    private static final Duration DEFAULT_COMPACTION_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    private final RecordWriter recordWriter;
    private final TableInfo tableInfo;
    private final HudiWriteTableInfo hudiTableInfo;
    private final CkpMetadata ckpMetadata;
    private final long compactionCompleteTimeoutMs;
    private final long compactionShutdownTimeoutMs;
    @Nullable private final ExecutorService compactionExecutor;
    @Nullable private final CompletableFuture<Map<String, List<WriteStatus>>> compactionFuture;

    public HudiLakeWriter(
            HudiCatalogProvider hudiCatalogProvider,
            CkpMetadataProvider ckpMetadataProvider,
            WriterInitContext writerInitContext)
            throws IOException {
        validateWriterInitContext(writerInitContext);
        this.tableInfo = writerInitContext.tableInfo();
        this.hudiTableInfo =
                HudiWriteTableInfo.create(hudiCatalogProvider, tableInfo.getTablePath());
        this.ckpMetadata = ckpMetadataProvider.get(tableInfo.getTablePath(), hudiTableInfo);
        this.compactionCompleteTimeoutMs =
                getTimeoutMillis(
                        hudiTableInfo.getFlinkConfig(),
                        COMPACTION_COMPLETE_TIMEOUT_KEY,
                        DEFAULT_COMPACTION_COMPLETE_TIMEOUT);
        this.compactionShutdownTimeoutMs =
                getTimeoutMillis(
                        hudiTableInfo.getFlinkConfig(),
                        COMPACTION_SHUTDOWN_TIMEOUT_KEY,
                        DEFAULT_COMPACTION_SHUTDOWN_TIMEOUT);

        if (writerInitContext.splitIndex() == 0) {
            ckpMetadata.bootstrap();
            initInstant(hudiTableInfo.getFlinkConfig(), hudiTableInfo.getMetaClient());
            LOG.info(
                    "Initialized Hudi instant for first split of table {}, bucket {}.",
                    tableInfo.getTablePath(),
                    writerInitContext.tableBucket());
        }

        this.recordWriter = new HudiRecordWriter(writerInitContext, hudiTableInfo, ckpMetadata);
        ExecutorService createdCompactionExecutor = null;
        if (shouldRunCompaction()) {
            createdCompactionExecutor =
                    Executors.newSingleThreadExecutor(
                            new ExecutorThreadFactory(
                                    "hudi-compact-" + writerInitContext.tableBucket()));
        }
        this.compactionExecutor = createdCompactionExecutor;
        LOG.info("Created HudiLakeWriter with configuration {}.", hudiTableInfo.getFlinkConfig());
        this.compactionFuture =
                compactionExecutor == null
                        ? null
                        : executeCompactionAsync(hudiCatalogProvider, writerInitContext);
    }

    @Override
    public void write(LogRecord record) throws IOException {
        try {
            recordWriter.write(record);
        } catch (Exception e) {
            throw new IOException("Failed to write Fluss record to Hudi.", e);
        }
    }

    @Override
    public HudiWriteResult complete() throws IOException {
        try {
            Map<String, List<WriteStatus>> writeStatuses = recordWriter.complete();
            Map<String, List<WriteStatus>> compactionWriteStatuses = Collections.emptyMap();
            if (compactionFuture != null) {
                compactionWriteStatuses = waitForCompaction();
            }
            return HudiWriteResult.fromWriteStatuses(writeStatuses, compactionWriteStatuses);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to complete Hudi write.", e);
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = closeCompactionExecutor();
        failure = close(failure, recordWriter, "Hudi record writer");
        failure = close(failure, ckpMetadata, "Hudi checkpoint metadata");
        if (failure != null) {
            throw failure;
        }
    }

    @Nullable
    private IOException closeCompactionExecutor() {
        if (compactionExecutor == null) {
            return null;
        }

        try {
            cancelCompaction();
            compactionExecutor.shutdown();
            if (!compactionExecutor.awaitTermination(
                    compactionShutdownTimeoutMs, TimeUnit.MILLISECONDS)) {
                compactionExecutor.shutdownNow();
                if (!compactionExecutor.awaitTermination(
                        compactionShutdownTimeoutMs, TimeUnit.MILLISECONDS)) {
                    return new IOException(
                            String.format(
                                    "Failed to close Hudi compaction executor within %s ms.",
                                    compactionShutdownTimeoutMs));
                }
            }
            return null;
        } catch (InterruptedException e) {
            compactionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            return new IOException("Interrupted while closing Hudi compaction executor.", e);
        } catch (Exception e) {
            return new IOException("Failed to close Hudi compaction executor.", e);
        }
    }

    private Map<String, List<WriteStatus>> waitForCompaction() throws IOException {
        try {
            return compactionFuture.get(compactionCompleteTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            cancelCompaction();
            throw new IOException(
                    String.format(
                            "Timed out after %s ms waiting for Hudi compaction to finish.",
                            compactionCompleteTimeoutMs),
                    e);
        } catch (InterruptedException e) {
            cancelCompaction();
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for Hudi compaction to finish.", e);
        } catch (ExecutionException e) {
            throw new IOException("Failed to execute Hudi compaction.", e.getCause());
        }
    }

    private void cancelCompaction() {
        if (compactionFuture != null && !compactionFuture.isDone()) {
            compactionFuture.cancel(true);
        }
    }

    @Nullable
    private static IOException close(
            @Nullable IOException failure, AutoCloseable closeable, String resourceName) {
        try {
            closeable.close();
            return failure;
        } catch (Exception e) {
            IOException closeException =
                    new IOException("Failed to close " + resourceName + ".", e);
            if (failure == null) {
                return closeException;
            }
            failure.addSuppressed(closeException);
            return failure;
        }
    }

    private boolean shouldRunCompaction() {
        return hudiTableInfo.getTableType() == HoodieTableType.MERGE_ON_READ
                && hudiTableInfo.getFlinkConfig().get(FlinkOptions.COMPACTION_SCHEDULE_ENABLED);
    }

    private CompletableFuture<Map<String, List<WriteStatus>>> executeCompactionAsync(
            HudiCatalogProvider hudiCatalogProvider, WriterInitContext writerInitContext) {
        return CompletableFuture.supplyAsync(
                () -> {
                    // Compaction runs in a separate thread, so it intentionally uses an isolated
                    // Hudi write client instead of sharing the data writer's client state.
                    try (HudiWriteTableInfo compactionTableInfo =
                            HudiWriteTableInfo.create(
                                    hudiCatalogProvider, writerInitContext.tablePath())) {
                        HudiCompactionService compactionService =
                                HudiCompactionService.forExecutor(
                                        compactionTableInfo,
                                        writerInitContext.tableBucket(),
                                        writerInitContext.partition());
                        List<String> instantTimes =
                                compactionService.getInflightCompactionInstantTimes();
                        List<Pair<String, HoodieCompactionPlan>> compactionPlans =
                                compactionService.getCompactionPlans(instantTimes);
                        return compactionService.executeCompaction(compactionPlans);
                    } catch (Exception e) {
                        throw new CompletionException(
                                String.format(
                                        "Failed to execute Hudi compaction for table %s, bucket %s.",
                                        writerInitContext.tablePath(),
                                        writerInitContext.tableBucket()),
                                e);
                    }
                },
                compactionExecutor);
    }

    private void initInstant(Configuration configuration, HoodieTableMetaClient metaClient) {
        metaClient.reloadActiveTimeline();
        WriteOperationType writeOperationType =
                WriteOperationType.fromValue(configuration.get(FlinkOptions.OPERATION));
        hudiTableInfo.getWriteClient().preTxn(writeOperationType, metaClient);

        String commitAction =
                CommitUtils.getCommitActionType(
                        writeOperationType,
                        HoodieTableType.valueOf(configuration.get(FlinkOptions.TABLE_TYPE)));
        String instant = hudiTableInfo.getWriteClient().startCommit(commitAction, metaClient);
        metaClient.getActiveTimeline().transitionRequestedToInflight(commitAction, instant);
        hudiTableInfo.getWriteClient().setWriteTimer(commitAction);
        ckpMetadata.startInstant(instant);
        LOG.info(
                "Created Hudi instant {} for table {} with type {}.",
                instant,
                tableInfo.getTablePath(),
                configuration.get(FlinkOptions.TABLE_TYPE));
    }

    private static void validateWriterInitContext(WriterInitContext writerInitContext) {
        checkArgument(
                writerInitContext.splitIndex() != UNKNOWN_SPLIT_INDEX,
                "Hudi lake writer requires split index in WriterInitContext.");
        checkArgument(
                writerInitContext.tieringRoundTimestamp() != UNKNOWN_TIERING_ROUND_TIMESTAMP,
                "Hudi lake writer requires tiering round timestamp in WriterInitContext.");
    }

    private static long getTimeoutMillis(Configuration config, String key, Duration defaultTimeout)
            throws IOException {
        try {
            Duration timeout =
                    TimeUtils.parseDuration(
                            config.getString(key, TimeUtils.getStringInMillis(defaultTimeout)));
            if (timeout.toMillis() <= 0) {
                throw new IllegalArgumentException("timeout must be greater than 0 ms");
            }
            return timeout.toMillis();
        } catch (RuntimeException e) {
            throw new IOException("Invalid Hudi compaction timeout option " + key + ".", e);
        }
    }
}
