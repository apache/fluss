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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.fluss.lake.iceberg.version.FormatVersionManager;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toPartition;

/** V3-aware delta writer using deletion vectors for position deletes when supported. */
public class V3DeltaTaskWriter extends BaseTaskWriter<Record> {

    private final V3EqualityDeltaWriter deltaWriter;

    public V3DeltaTaskWriter(
            Table icebergTable,
            Schema deleteSchema,
            FileFormat format,
            FileAppenderFactory<Record> appenderFactory,
            OutputFileFactory fileFactory,
            FileIO io,
            long targetFileSize,
            @Nullable String partition,
            int bucket) {
        super(icebergTable.spec(), format, appenderFactory, fileFactory, io, targetFileSize);

        boolean useDeletionVectors = FormatVersionManager.supportsDeletionVectors(icebergTable);

        this.deltaWriter =
                new V3EqualityDeltaWriter(
                        toPartition(icebergTable, partition, bucket),
                        icebergTable.schema(),
                        deleteSchema,
                        icebergTable.spec(),
                        useDeletionVectors,
                        fileFactory);
    }

    @Override
    public void write(Record row) throws IOException {
        deltaWriter.write(row);
    }

    public void delete(Record row) throws IOException {
        deltaWriter.delete(row);
    }

    @Override
    public WriteResult complete() throws IOException {
        WriteResult baseResult = super.complete();

        List<DeleteFile> allDeleteFiles = Lists.newArrayList(baseResult.deleteFiles());
        allDeleteFiles.addAll(deltaWriter.getDvDeleteFiles());

        return WriteResult.builder()
                .addDataFiles(baseResult.dataFiles())
                .addDeleteFiles(allDeleteFiles)
                .addReferencedDataFiles(baseResult.referencedDataFiles())
                .build();
    }

    @Override
    public void close() throws IOException {
        deltaWriter.close();
    }

    /** Equality delta writer with optional DV support for intra-batch position deletes. */
    private class V3EqualityDeltaWriter extends BaseEqualityDeltaWriter {
        private final boolean useDeletionVectors;
        private final DVFileWriter dvWriter;
        private final PartitionSpec spec;
        private final StructLike partition;
        private DeleteWriteResult dvResult;

        V3EqualityDeltaWriter(
                PartitionKey partitionKey,
                Schema schema,
                Schema eqDeleteSchema,
                PartitionSpec spec,
                boolean useDeletionVectors,
                OutputFileFactory fileFactory) {
            super(partitionKey, schema, eqDeleteSchema);
            this.spec = spec;
            this.partition = partitionKey;
            this.useDeletionVectors = useDeletionVectors;

            if (useDeletionVectors) {
                Function<String, PositionDeleteIndex> loadPreviousDeletes = path -> null;
                this.dvWriter = new BaseDVFileWriter(fileFactory, loadPreviousDeletes);
            } else {
                this.dvWriter = null;
            }
        }

        @Override
        protected StructLike asStructLike(Record record) {
            return record;
        }

        @Override
        protected StructLike asStructLikeKey(Record record) {
            return record;
        }

        List<DeleteFile> getDvDeleteFiles() {
            if (dvResult != null) {
                return dvResult.deleteFiles();
            }
            return Lists.newArrayList();
        }

        @Override
        public void close() throws IOException {
            super.close();
            if (dvWriter != null) {
                dvWriter.close();
                dvResult = dvWriter.result();
            }
        }
    }
}
