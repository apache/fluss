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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.PartitionDoneHandler;
import org.apache.fluss.lake.committer.PartitionDoneInitContext;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.utils.AutoPartitionStrategy;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.apache.paimon.table.FileStoreTable;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;

/** Implementation of {@link LakeTieringFactory} for Paimon . */
public class PaimonLakeTieringFactory
        implements LakeTieringFactory<PaimonWriteResult, PaimonCommittable> {

    private static final long serialVersionUID = 1L;

    /**
     * Paimon-native property ({@code paimon.}-prefixed) that both enables lake partition mark-done
     * and configures the idle time (Paimon's {@code partition.idle-time-to-done}).
     */
    public static final String PARTITION_IDLE_TIME_TO_DONE_KEY =
            "paimon.partition.idle-time-to-done";

    private final PaimonCatalogProvider paimonCatalogProvider;

    public PaimonLakeTieringFactory(Configuration paimonConfig) {
        this.paimonCatalogProvider = new PaimonCatalogProvider(paimonConfig);
    }

    @Override
    public LakeWriter<PaimonWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        return new PaimonLakeWriter(paimonCatalogProvider, writerInitContext);
    }

    @Override
    public SimpleVersionedSerializer<PaimonWriteResult> getWriteResultSerializer() {
        return new PaimonWriteResultSerializer();
    }

    @Override
    public LakeCommitter<PaimonWriteResult, PaimonCommittable> createLakeCommitter(
            CommitterInitContext committerInitContext) throws IOException {
        return new PaimonLakeCommitter(paimonCatalogProvider, committerInitContext);
    }

    @Override
    public SimpleVersionedSerializer<PaimonCommittable> getCommittableSerializer() {
        return new PaimonCommittableSerializer();
    }

    @Override
    public Optional<PartitionDoneHandler> createPartitionDoneHandler(
            PartitionDoneInitContext partitionDoneInitContext) {
        TableInfo tableInfo = partitionDoneInitContext.tableInfo();
        // only partitioned tables with the mark-done feature enabled are supported. It is enabled
        // when Paimon's native partition.idle-time-to-done (paimon.-prefixed) property is set.
        if (!tableInfo.isPartitioned()
                || !tableInfo.getCustomProperties().containsKey(PARTITION_IDLE_TIME_TO_DONE_KEY)) {
            return Optional.empty();
        }

        Configuration customProps = tableInfo.getCustomProperties();
        // Fluss reads Paimon's native partition.idle-time-to-done (paimon.-prefixed in the Fluss
        // table properties) to judge idle partitions. Its presence is guaranteed by the enabled
        // check above.
        long idleTimeMs =
                customProps
                        .getOptional(
                                ConfigBuilder.key(PARTITION_IDLE_TIME_TO_DONE_KEY)
                                        .durationType()
                                        .noDefaultValue())
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Missing " + PARTITION_IDLE_TIME_TO_DONE_KEY))
                        .toMillis();

        Catalog catalog = paimonCatalogProvider.get();
        try {
            FileStoreTable fileStoreTable =
                    (FileStoreTable)
                            catalog.getTable(toPaimon(partitionDoneInitContext.tablePath()));
            // The Paimon table already carries the mark-done options transferred from the Fluss
            // table properties (paimon.partition.mark-done-action* -> partition.mark-done-action*)
            // at table creation, so we can directly reuse Paimon's native createActions.
            CoreOptions coreOptions = fileStoreTable.coreOptions();
            List<PartitionMarkDoneAction> actions =
                    PartitionMarkDoneAction.createActions(
                            Thread.currentThread().getContextClassLoader(),
                            fileStoreTable,
                            coreOptions);

            AutoPartitionStrategy autoPartitionStrategy =
                    tableInfo.getTableConfig().getAutoPartitionStrategy();
            if (autoPartitionStrategy.isAutoPartitionEnabled()) {
                // auto-partition table: use Fluss auto-partition format and the time unit to
                // compute the exact partition end time.
                return Optional.of(
                        new PaimonPartitionDoneHandler(
                                actions,
                                tableInfo.getPartitionKeys(),
                                idleTimeMs,
                                autoPartitionStrategy.timeZone().toZoneId(),
                                autoPartitionStrategy.timeUnit(),
                                null));
            } else {
                // non-auto-partition table: use Paimon's PartitionTimeExtractor with the native
                // timestamp-pattern / timestamp-formatter (paimon.-prefixed) to get the partition
                // time used in the idle judgement.
                PartitionTimeExtractor timeExtractor =
                        new PartitionTimeExtractor(
                                customProps
                                        .getOptional(
                                                ConfigBuilder.key(
                                                                "paimon.partition.timestamp-pattern")
                                                        .stringType()
                                                        .noDefaultValue())
                                        .orElse(null),
                                customProps
                                        .getOptional(
                                                ConfigBuilder.key(
                                                                "paimon.partition.timestamp-formatter")
                                                        .stringType()
                                                        .noDefaultValue())
                                        .orElse(null));
                return Optional.of(
                        new PaimonPartitionDoneHandler(
                                actions,
                                tableInfo.getPartitionKeys(),
                                idleTimeMs,
                                ZoneId.systemDefault(),
                                null,
                                timeExtractor));
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create PaimonPartitionDoneHandler for table "
                            + partitionDoneInitContext.tablePath(),
                    e);
        }
    }
}
