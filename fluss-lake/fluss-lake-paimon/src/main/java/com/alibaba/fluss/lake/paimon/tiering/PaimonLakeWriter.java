/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.lake.paimon.tiering.append.AppendOnlyWriter;
import com.alibaba.fluss.lake.paimon.tiering.mergetree.MergeTreeWriter;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecord;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;

import java.io.IOException;
import java.util.List;

import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimon;

/** Implementation of {@link LakeWriter} for Paimon. */
public class PaimonLakeWriter implements LakeWriter<PaimonWriteResult> {

    private final Catalog paimonCatalog;
    private final RecordWriter<?> recordWriter;

    public PaimonLakeWriter(
            PaimonCatalogProvider paimonCatalogProvider, WriterInitContext writerInitContext)
            throws IOException {
        this.paimonCatalog = paimonCatalogProvider.get();
        FileStoreTable fileStoreTable = getTable(writerInitContext.tablePath());

        List<String> partitionKeys = fileStoreTable.partitionKeys();

        this.recordWriter =
                fileStoreTable.primaryKeys().isEmpty()
                        ? new AppendOnlyWriter(
                                fileStoreTable,
                                writerInitContext.tableBucket(),
                                writerInitContext.partition(),
                                partitionKeys)
                        : new MergeTreeWriter(
                                fileStoreTable,
                                writerInitContext.tableBucket(),
                                writerInitContext.partition(),
                                partitionKeys);
    }

    @Override
    public void write(LogRecord record) throws IOException {
        try {
            recordWriter.write(record);
        } catch (Exception e) {
            throw new IOException("Fail to write Fluss record to Paimon.", e);
        }
    }

    @Override
    public PaimonWriteResult complete() throws IOException {
        CommitMessage commitMessage;
        try {
            commitMessage = recordWriter.complete();
        } catch (Exception e) {
            throw new IOException("Fail to complete Paimon write.", e);
        }
        return new PaimonWriteResult(commitMessage);
    }

    @Override
    public void close() throws IOException {
        try {
            if (recordWriter != null) {
                recordWriter.close();
            }
            if (paimonCatalog != null) {
                paimonCatalog.close();
            }
        } catch (Exception e) {
            throw new IOException("Fail to close PaimonLakeWriter.", e);
        }
    }

    private FileStoreTable getTable(TablePath tablePath) throws IOException {
        try {
            return (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        } catch (Exception e) {
            throw new IOException("Fail to get table " + tablePath + " in Paimon.");
        }
    }
}
