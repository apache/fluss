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

package com.alibaba.fluss.lake.lance.utils;

import com.alibaba.fluss.lake.lance.LanceConfig;
import com.alibaba.fluss.lake.lance.tiering.LanceArrowWriter;
import com.alibaba.fluss.types.RowType;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.Transaction;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.operation.Append;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Lance dataset API adapter. */
public class LanceDatasetAdapter {
    private static final BufferAllocator allocator = new RootAllocator();

    public static void createDataset(String datasetUri, Schema schema, WriteParams params) {
        Dataset.create(allocator, datasetUri, schema, params).close();
    }

    public static Optional<Schema> getSchema(LanceConfig config) {
        String uri = config.getDatasetUri();
        ReadOptions options = LanceConfig.genReadOptionFromConfig(config);
        try (Dataset dataset = Dataset.open(allocator, uri, options)) {
            return Optional.of(dataset.getSchema());
        } catch (IllegalArgumentException e) {
            // dataset not found
            return Optional.empty();
        }
    }

    public static long commitAppend(
            LanceConfig config, List<FragmentMetadata> fragments, Map<String, String> properties) {
        String uri = config.getDatasetUri();
        ReadOptions options = LanceConfig.genReadOptionFromConfig(config);
        try (Dataset dataset = Dataset.open(allocator, uri, options)) {
            Transaction transaction =
                    dataset.newTransactionBuilder()
                            .operation(Append.builder().fragments(fragments).build())
                            .transactionProperties(properties)
                            .build();
            try (Dataset appendedDataset = transaction.commit()) {
                // note: lance dataset version starts from 1
                return appendedDataset.version();
            }
        }
    }

    public static LanceArrowWriter getArrowWriter(Schema schema, int batchSize, RowType rowType) {
        return new LanceArrowWriter(allocator, schema, batchSize, rowType);
    }

    public static List<FragmentMetadata> createFragment(
            String datasetUri, ArrowReader reader, WriteParams params) {
        try (ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
            Data.exportArrayStream(allocator, reader, arrowStream);
            return Fragment.create(datasetUri, arrowStream, params);
        }
    }
}
