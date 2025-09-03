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

import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/** Implementation of {@link LakeWriter} for Lance. */
public class LanceLakeWriter implements LakeWriter<LanceWriteResult> {
    private final LanceArrowWriter arrowWriter;
    private final FutureTask<List<FragmentMetadata>> fragmentCreationTask;

    public LanceLakeWriter(Configuration options, WriterInitContext writerInitContext)
            throws IOException {
        LanceConfig config =
                LanceConfig.from(
                        options.toMap(),
                        writerInitContext.tableInfo().getCustomProperties().toMap(),
                        writerInitContext.tablePath().getDatabaseName(),
                        writerInitContext.tablePath().getTableName());
        int batchSize = LanceConfig.getBatchSize(config);
        Optional<Schema> schema = LanceDatasetAdapter.getSchema(config);
        if (!schema.isPresent()) {
            throw new IOException("Fail to get dataset " + config.getDatasetUri() + " in Lance.");
        }

        this.arrowWriter =
                LanceDatasetAdapter.getArrowWriter(
                        schema.get(), batchSize, writerInitContext.tableInfo().getRowType());

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
