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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingDataWriter;
import org.apache.iceberg.io.RollingEqualityDeleteWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructLikeUtil;
import org.apache.iceberg.util.StructProjection;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toPartition;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A V3 delta writer that uses deletion vectors for rows inserted by the same task.
 *
 * <p>Keys not inserted by this task still produce equality deletes. In particular, these DVs never
 * modify existing data files, so there are no previously committed DVs to load or replace.
 */
class GenericRecordDVWriter implements DeltaWriter {

    private final PartitionSpec spec;
    @Nullable private final StructLike partition;
    private final InternalRecordWrapper wrapper;
    private final StructProjection keyProjection;
    private final StructLikeMap<PathOffset> insertedRows;
    private final TrackingFileIO io;
    private final RollingDataWriter<Record> dataWriter;
    private final RollingEqualityDeleteWriter<Record> equalityWriter;
    private final BaseDVFileWriter dvWriter;

    private boolean closed;
    private boolean aborted;
    @Nullable private IOException failure;

    GenericRecordDVWriter(
            Table table,
            Schema deleteSchema,
            FileFormat format,
            FileWriterFactory<Record> writerFactory,
            long targetFileSize,
            @Nullable String partitionName,
            int bucket) {
        this.spec = table.spec();
        this.partition = toPartition(table, partitionName, bucket);
        this.wrapper = new InternalRecordWrapper(table.schema().asStruct());
        this.keyProjection = StructProjection.create(table.schema(), deleteSchema);
        this.insertedRows = StructLikeMap.create(deleteSchema.asStruct());
        this.io = new TrackingFileIO(table.io());
        OutputFileFactory fileFactory =
                OutputFileFactory.builderFor(table, bucket, 0)
                        .format(format)
                        .ioSupplier(() -> io)
                        .build();
        this.dataWriter =
                new RollingDataWriter<>(
                        writerFactory, fileFactory, io, targetFileSize, spec, partition);
        this.equalityWriter =
                new RollingEqualityDeleteWriter<>(
                        writerFactory, fileFactory, io, targetFileSize, spec, partition);
        this.dvWriter =
                new BaseDVFileWriter(
                        OutputFileFactory.builderFor(table, bucket, 0)
                                .format(FileFormat.PUFFIN)
                                .ioSupplier(() -> io)
                                .build(),
                        path -> null);
    }

    @Override
    public void write(Record row) throws IOException {
        checkOpen();
        try {
            // Capture the position before write(), which can roll to the next data file.
            PathOffset current =
                    new PathOffset(
                            dataWriter.currentFilePath().toString(), dataWriter.currentFileRows());
            StructLike key = StructLikeUtil.copy(keyProjection.wrap(wrapper.wrap(row)));
            PathOffset previous = insertedRows.put(key, current);
            if (previous != null) {
                dvWriter.delete(previous.path, previous.offset, spec, partition);
            }
            dataWriter.write(row);
        } catch (RuntimeException e) {
            failure = new IOException("Failed to write an Iceberg V3 record", e);
            throw e;
        }
    }

    @Override
    public void delete(Record row) throws IOException {
        checkOpen();
        try {
            PathOffset previous = insertedRows.remove(keyProjection.wrap(wrapper.wrap(row)));
            if (previous == null) {
                equalityWriter.write(row);
            } else {
                dvWriter.delete(previous.path, previous.offset, spec, partition);
            }
        } catch (RuntimeException e) {
            failure = new IOException("Failed to delete an Iceberg V3 record", e);
            throw e;
        }
    }

    @Override
    public WriteResult complete() throws IOException {
        checkState(!aborted, "Cannot complete an aborted writer");
        close();
        DeleteWriteResult dvResult = dvWriter.result();
        return WriteResult.builder()
                .addDataFiles(dataWriter.result().dataFiles())
                .addDeleteFiles(equalityWriter.result().deleteFiles())
                .addDeleteFiles(dvResult.deleteFiles())
                .addReferencedDataFiles(dvResult.referencedDataFiles())
                .build();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            closeWriter(dataWriter);
            closeWriter(equalityWriter);
            closeWriter(dvWriter);
            insertedRows.clear();
        }
        if (failure != null) {
            deleteOutputs();
            throw failure;
        }
    }

    @Override
    public void abort() throws IOException {
        aborted = true;
        try {
            close();
        } finally {
            deleteOutputs();
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void checkOpen() throws IOException {
        checkState(!closed, "Cannot write to a closed writer");
        if (failure != null) {
            throw failure;
        }
    }

    private void closeWriter(Closeable writer) {
        try {
            writer.close();
        } catch (IOException | RuntimeException e) {
            recordFailure(e);
        }
    }

    private void deleteOutputs() {
        // Track output creation, not just completed results: a failed Puffin close may leave
        // a partial file without any DeleteFile metadata. Shared Puffin files are deleted once.
        for (String location : new ArrayList<>(io.outputs)) {
            try {
                io.deleteFile(location);
            } catch (RuntimeException e) {
                recordFailure(e);
            }
        }
    }

    private void recordFailure(Exception e) {
        if (failure == null) {
            failure = new IOException("Failed to close or abort the Iceberg V3 writer", e);
        } else {
            failure.addSuppressed(e);
        }
    }

    private static class PathOffset {
        private final String path;
        private final long offset;

        private PathOffset(String path, long offset) {
            this.path = path;
            this.offset = offset;
        }
    }

    /** Tracks only files owned by this task, including files whose creation or close failed. */
    private static class TrackingFileIO implements FileIO {
        private final FileIO delegate;
        private final Set<String> outputs = new HashSet<>();

        private TrackingFileIO(FileIO delegate) {
            this.delegate = delegate;
        }

        @Override
        public InputFile newInputFile(String path) {
            return delegate.newInputFile(path);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            outputs.add(path);
            return delegate.newOutputFile(path);
        }

        @Override
        public void deleteFile(String path) {
            delegate.deleteFile(path);
            outputs.remove(path);
        }
    }
}
