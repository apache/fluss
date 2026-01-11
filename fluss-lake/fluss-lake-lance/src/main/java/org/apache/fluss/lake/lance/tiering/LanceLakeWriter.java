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

package org.apache.fluss.lake.lance.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lance.LanceConfig;
import org.apache.fluss.lake.lance.utils.LanceDatasetAdapter;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Implementation of {@link LakeWriter} for Lance using batch processing. */
public class LanceLakeWriter implements LakeWriter<LanceWriteResult> {
    private final BufferAllocator allocator;
    private final Schema arrowSchema;
    private final RowType rowType;
    private final int batchSize;
    private final String datasetUri;
    private final WriteParams writeParams;

    private final List<InternalRow> buffer;
    private List<FragmentMetadata> allFragments;

    public LanceLakeWriter(Configuration options, WriterInitContext writerInitContext)
            throws IOException {
        LanceConfig config =
                LanceConfig.from(
                        options.toMap(),
                        writerInitContext.tableInfo().getCustomProperties().toMap(),
                        writerInitContext.tablePath().getDatabaseName(),
                        writerInitContext.tablePath().getTableName());

        this.batchSize = LanceConfig.getBatchSize(config);
        this.datasetUri = config.getDatasetUri();
        this.writeParams = LanceConfig.genWriteParamsFromConfig(config);
        this.rowType = writerInitContext.tableInfo().getRowType();
        this.allocator = new RootAllocator();
        this.buffer = new ArrayList<>(batchSize);
        this.allFragments = new ArrayList<>();

        Optional<Schema> schema = LanceDatasetAdapter.getSchema(config);
        if (!schema.isPresent()) {
            throw new IOException("Fail to get dataset " + datasetUri + " in Lance.");
        }
        this.arrowSchema = schema.get();
    }

    @Override
    public void write(LogRecord record) throws IOException {
        buffer.add(record.getRow());

        // Flush when buffer reaches batch size
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    private void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }

        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            // Allocate memory and write data
            root.allocateNew();
            LanceArrowWriter writer = LanceArrowWriter.create(root, rowType);

            for (InternalRow row : buffer) {
                writer.writeRow(row);
            }
            writer.finish();

            // Create fragment directly using VectorSchemaRoot
            List<FragmentMetadata> fragments =
                    Fragment.create(datasetUri, allocator, root, writeParams);

            allFragments.addAll(fragments);
            buffer.clear();
        } catch (Exception e) {
            throw new IOException("Failed to write Lance fragment", e);
        }
    }

    @Override
    public LanceWriteResult complete() throws IOException {
        // Flush any remaining data
        flush();
        return new LanceWriteResult(allFragments);
    }

    @Override
    public void close() throws IOException {
        buffer.clear();
        if (allocator != null) {
            allocator.close();
        }
    }
}
