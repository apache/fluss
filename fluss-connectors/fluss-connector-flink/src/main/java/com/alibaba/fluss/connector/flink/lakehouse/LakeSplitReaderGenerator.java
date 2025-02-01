/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.connector.flink.lakehouse;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.connector.flink.lakehouse.paimon.reader.PaimonSnapshotAndLogSplitScanner;
import com.alibaba.fluss.connector.flink.lakehouse.paimon.reader.PaimonSnapshotScanner;
import com.alibaba.fluss.connector.flink.lakehouse.paimon.split.PaimonSnapshotAndFlussLogSplit;
import com.alibaba.fluss.connector.flink.lakehouse.paimon.split.PaimonSnapshotSplit;
import com.alibaba.fluss.connector.flink.source.reader.BoundedSplitReader;
import com.alibaba.fluss.connector.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.lakehouse.LakeStorageInfo;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;

import javax.annotation.Nullable;

import java.util.Queue;
import java.util.stream.IntStream;

/** A generator to generate reader for lake split. */
public class LakeSplitReaderGenerator {

    private final Table table;
    private final Connection connection;

    private final TablePath tablePath;
    private FileStoreTable fileStoreTable;
    private final @Nullable int[] projectedFields;

    public LakeSplitReaderGenerator(
            Table table,
            Connection connection,
            TablePath tablePath,
            @Nullable int[] projectedFields) {
        this.table = table;
        this.connection = connection;
        this.tablePath = tablePath;
        this.projectedFields = projectedFields;
    }

    public void addSplit(SourceSplitBase split, Queue<SourceSplitBase> boundedSplits) {
        if (split instanceof PaimonSnapshotSplit) {
            boundedSplits.add(split);
        } else if (split instanceof PaimonSnapshotAndFlussLogSplit) {
            boundedSplits.add(split);
        } else {
            throw new UnsupportedOperationException(
                    String.format("The split type of %s is not supported.", split.getClass()));
        }
    }

    public BoundedSplitReader getBoundedSplitScanner(SourceSplitBase split) {
        if (split instanceof PaimonSnapshotSplit) {
            PaimonSnapshotSplit paimonSnapshotSplit = (PaimonSnapshotSplit) split;
            FileStoreTable paimonStoreTable = getFileStoreTable();
            int[] projectedFields = getProjectedFieldsForPaimonTable(table);
            ReadBuilder readBuilder =
                    paimonStoreTable.newReadBuilder().withProjection(projectedFields);
            PaimonSnapshotScanner paimonSnapshotScanner =
                    new PaimonSnapshotScanner(
                            readBuilder.newRead(), paimonSnapshotSplit.getFileStoreSourceSplit());
            return new BoundedSplitReader(
                    paimonSnapshotScanner,
                    paimonSnapshotSplit.getFileStoreSourceSplit().recordsToSkip());
        } else if (split instanceof PaimonSnapshotAndFlussLogSplit) {
            PaimonSnapshotAndFlussLogSplit paimonSnapshotAndFlussLogSplit =
                    (PaimonSnapshotAndFlussLogSplit) split;
            FileStoreTable paimonStoreTable = getFileStoreTable();
            PaimonSnapshotAndLogSplitScanner paimonSnapshotAndLogSplitScanner =
                    new PaimonSnapshotAndLogSplitScanner(
                            table,
                            paimonStoreTable,
                            paimonSnapshotAndFlussLogSplit,
                            projectedFields);
            return new BoundedSplitReader(
                    paimonSnapshotAndLogSplitScanner,
                    paimonSnapshotAndFlussLogSplit.getRecordsToSkip());
        } else {
            throw new UnsupportedOperationException(
                    String.format("The split type of %s is not supported.", split.getClass()));
        }
    }

    private int[] getProjectedFieldsForPaimonTable(Table flussTable) {
        return this.projectedFields != null
                ? this.projectedFields
                // only read the field in origin fluss table, not include log_offset, log_timestamp
                // fields
                : IntStream.range(0, flussTable.getDescriptor().getSchema().getColumnNames().size())
                        .toArray();
    }

    private FileStoreTable getFileStoreTable() {
        if (fileStoreTable != null) {
            return fileStoreTable;
        }

        try (Admin admin = connection.getAdmin()) {
            LakeStorageInfo dataLakeInfo = admin.describeLakeStorage().get();
            try (Catalog paimonCatalog =
                    FlinkCatalogFactory.createPaimonCatalog(
                            Options.fromMap(dataLakeInfo.getCatalogProperties()))) {
                fileStoreTable =
                        (FileStoreTable)
                                paimonCatalog.getTable(
                                        Identifier.create(
                                                tablePath.getDatabaseName(),
                                                tablePath.getTableName()));
            }
            return fileStoreTable;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Fail to get paimon table.", ExceptionUtils.stripExecutionException(e));
        }
    }
}
