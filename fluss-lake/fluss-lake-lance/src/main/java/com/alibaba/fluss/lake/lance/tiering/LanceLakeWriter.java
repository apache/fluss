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

package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.lance.LanceConfig;
import com.alibaba.fluss.lake.lance.utils.LanceDatasetAdapter;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** Implementation of {@link LakeWriter} for Lance. */
public class LanceLakeWriter implements LakeWriter<LanceWriteResult> {
    private final LanceArrowWriter arrowWriter;
    FutureTask<List<FragmentMetadata>> fragmentCreationTask;

    public LanceLakeWriter(Configuration options, WriterInitContext writerInitContext)
            throws IOException {
        LanceConfig config =
                LanceConfig.from(
                        options.toMap(),
                        writerInitContext.tablePath().getDatabaseName(),
                        writerInitContext.tablePath().getTableName());
        int batchSize = LanceConfig.getBatchSize(config);
        Optional<Schema> schema = LanceDatasetAdapter.getSchema(config);
        if (!schema.isPresent()) {
            throw new IOException("Fail to get dataset " + config.getDatasetUri() + " in Lance.");
        }

        RowType.Builder rowTypeBuilder = RowType.builder();
        for (DataField field : writerInitContext.schema().getRowType().getFields()) {
            rowTypeBuilder.field(field.getName(), field.getType());
        }
        rowTypeBuilder.field(BUCKET_COLUMN_NAME, DataTypes.INT());
        rowTypeBuilder.field(OFFSET_COLUMN_NAME, DataTypes.BIGINT());
        rowTypeBuilder.field(TIMESTAMP_COLUMN_NAME, DataTypes.TIMESTAMP_LTZ());

        this.arrowWriter =
                LanceDatasetAdapter.getArrowWriter(
                        schema.get(),
                        batchSize,
                        writerInitContext.tableBucket(),
                        rowTypeBuilder.build());

        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);
        Callable<List<FragmentMetadata>> fragmentCreator =
                () ->
                        LanceDatasetAdapter.createFragment(
                                config.getDatasetUri(), arrowWriter, params);
        fragmentCreationTask = new FutureTask<>(fragmentCreator);
        Thread fragmentCreationThread = new Thread(fragmentCreationTask);
        fragmentCreationThread.start();
    }

    @Override
    public void write(LogRecord record) throws IOException {
        arrowWriter.write(record);
    }

    @Override
    public LanceWriteResult complete() throws IOException {
        arrowWriter.setFinished();
        try {
            List<FragmentMetadata> fragmentMetadata = fragmentCreationTask.get();
            return new LanceWriteResult(fragmentMetadata);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for reader thread to finish", e);
        } catch (ExecutionException e) {
            throw new IOException("Exception in reader thread", e);
        }
    }

    @Override
    public void close() throws IOException {
        arrowWriter.close();
    }
}
