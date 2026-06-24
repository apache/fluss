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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.lake.writer.WriterInitContext.UNKNOWN_SPLIT_INDEX;
import static org.apache.fluss.lake.writer.WriterInitContext.UNKNOWN_TIERING_ROUND_TIMESTAMP;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Hudi implementation of {@link LakeWriter}. */
public class HudiLakeWriter implements LakeWriter<HudiWriteResult> {

    private static final Logger LOG = LoggerFactory.getLogger(HudiLakeWriter.class);

    private final RecordWriter recordWriter;
    private final TableInfo tableInfo;
    private final HudiWriteTableInfo hudiTableInfo;
    private final CkpMetadata ckpMetadata;
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

        if (writerInitContext.splitIndex() == 0) {
            ckpMetadata.bootstrap();
            initInstant(hudiTableInfo.getFlinkConfig(), hudiTableInfo.getMetaClient());
            LOG.info(
                    "Initialized Hudi instant for first split of table {}, bucket {}.",
                    tableInfo.getTablePath(),
                    writerInitContext.tableBucket());
        }

        this.recordWriter = new HudiRecordWriter(writerInitContext, hudiTableInfo, ckpMetadata);
        if (shouldRunCompaction(writerInitContext)) {
            this.compactionExecutor =
                    Executors.newSingleThreadExecutor(
                            new ExecutorThreadFactory(
                                    "hudi-compact-" + writerInitContext.tableBucket()));
            this.compactionFuture = executeCompactionAsync(hudiCatalogProvider, writerInitContext);
        } else {
            this.compactionExecutor = null;
            this.compactionFuture = null;
        }
        LOG.info("Created HudiLakeWriter with configuration {}.", hudiTableInfo.getFlinkConfig());
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
                compactionWriteStatuses = compactionFuture.get();
            }
            return HudiWriteResult.fromWriteStatuses(writeStatuses, compactionWriteStatuses);
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
        try {
            if (compactionFuture != null && !compactionFuture.isDone()) {
                compactionFuture.cancel(true);
            }
            if (compactionExecutor != null) {
                compactionExecutor.shutdown();
                if (!compactionExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    LOG.warn("Failed to close Hudi compaction executor.");
                }
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new IOException("Interrupted while closing Hudi compaction executor.", e);
        } catch (Exception e) {
            return new IOException("Failed to close Hudi compaction executor.", e);
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

    private boolean shouldRunCompaction(WriterInitContext writerInitContext) {
        return writerInitContext.tableInfo().getTableConfig().isDataLakeAutoCompaction()
                && hudiTableInfo.getTableType() == HoodieTableType.MERGE_ON_READ;
    }

    private CompletableFuture<Map<String, List<WriteStatus>>> executeCompactionAsync(
            HudiCatalogProvider hudiCatalogProvider, WriterInitContext writerInitContext) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try (HudiWriteTableInfo compactionTableInfo =
                            HudiWriteTableInfo.create(
                                    hudiCatalogProvider, writerInitContext.tablePath())) {
                        HudiCompactionService compactionService =
                                new HudiCompactionService(
                                        compactionTableInfo,
                                        writerInitContext.tableBucket(),
                                        writerInitContext.partition());
                        List<String> instantTimes =
                                compactionService.getInflightCompactionInstantTimes();
                        List<Pair<String, HoodieCompactionPlan>> compactionPlans =
                                compactionService.getCompactionPlans(instantTimes);
                        return compactionService.executeCompaction(compactionPlans);
                    } catch (Exception e) {
                        LOG.warn(
                                "Failed to execute Hudi compaction for table {}, bucket {}.",
                                writerInitContext.tablePath(),
                                writerInitContext.tableBucket(),
                                e);
                        return Collections.emptyMap();
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
}
