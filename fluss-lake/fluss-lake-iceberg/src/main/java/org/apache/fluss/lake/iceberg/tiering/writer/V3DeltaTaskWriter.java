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
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructLikeUtil;
import org.apache.iceberg.util.StructProjection;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toPartition;

/** V3-aware delta writer using deletion vectors for position deletes when supported. */
public class V3DeltaTaskWriter extends BaseTaskWriter<Record> {

    private final BaseEqualityDeltaWriter deltaWriter;
    private final boolean useDeletionVectors;

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

        this.useDeletionVectors = FormatVersionManager.supportsDeletionVectors(icebergTable);
        PartitionKey partitionKey = toPartition(icebergTable, partition, bucket);

        if (useDeletionVectors) {
            this.deltaWriter =
                    new V3DVWriter(
                            partitionKey,
                            icebergTable.schema(),
                            deleteSchema,
                            icebergTable.spec(),
                            fileFactory);
        } else {
            this.deltaWriter =
                    new V2EqualityDeltaWriter(partitionKey, icebergTable.schema(), deleteSchema);
        }
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

        if (useDeletionVectors && deltaWriter instanceof V3DVWriter) {
            allDeleteFiles.addAll(((V3DVWriter) deltaWriter).getDvDeleteFiles());
        }

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

    /** V2 equality delta writer - uses base class position delete handling. */
    private class V2EqualityDeltaWriter extends BaseEqualityDeltaWriter {
        V2EqualityDeltaWriter(PartitionKey partition, Schema schema, Schema eqDeleteSchema) {
            super(partition, schema, eqDeleteSchema);
        }

        @Override
        protected StructLike asStructLike(Record record) {
            return record;
        }

        @Override
        protected StructLike asStructLikeKey(Record record) {
            return record;
        }
    }

    /** V3 writer using DVFileWriter for position deletes instead of base class handling. */
    private class V3DVWriter extends BaseEqualityDeltaWriter {
        private final DVFileWriter dvWriter;
        private final StructProjection keyProjection;
        private final PartitionSpec spec;
        private final StructLike partition;
        private final RollingFileWriter dataWriter;
        private Map<StructLike, PathOffset> positionMap;
        private DeleteWriteResult dvResult;

        V3DVWriter(
                PartitionKey partitionKey,
                Schema schema,
                Schema eqDeleteSchema,
                PartitionSpec spec,
                OutputFileFactory fileFactory) {
            super(partitionKey, schema, eqDeleteSchema);
            this.keyProjection = StructProjection.create(schema, eqDeleteSchema);
            this.spec = spec;
            this.partition = partitionKey;
            this.dataWriter = new RollingFileWriter(partitionKey);
            this.positionMap = StructLikeMap.create(eqDeleteSchema.asStruct());

            Function<String, PositionDeleteIndex> loadPreviousDeletes = path -> null;
            this.dvWriter = new BaseDVFileWriter(fileFactory, loadPreviousDeletes);
        }

        @Override
        public void write(Record row) throws IOException {
            CharSequence path = dataWriter.currentPath();
            long pos = dataWriter.currentRows();
            StructLike key = StructLikeUtil.copy(keyProjection.wrap(row));

            PathOffset previous = positionMap.put(key, new PathOffset(path, pos));
            if (previous != null) {
                dvWriter.delete(previous.path.toString(), previous.rowOffset, spec, partition);
            }
            dataWriter.write(row);
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
            return dvResult != null ? dvResult.deleteFiles() : Lists.newArrayList();
        }

        @Override
        public void close() throws IOException {
            if (dataWriter != null) {
                dataWriter.close();
            }
            if (dvWriter != null) {
                dvWriter.close();
                dvResult = dvWriter.result();
            }
            if (positionMap != null) {
                positionMap.clear();
                positionMap = null;
            }
        }

        private class PathOffset {
            final CharSequence path;
            final long rowOffset;

            PathOffset(CharSequence path, long rowOffset) {
                this.path = path;
                this.rowOffset = rowOffset;
            }
        }
    }
}
