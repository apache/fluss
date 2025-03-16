/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.lakehouse.paimon.sink;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.paimon.record.MultiplexCdcRecord;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Builder of the paimon sink for syncing the tables from Fluss. */
public class PaimonDataBaseSyncSinkBuilder {

    private final Options catalogOptions;
    private final Configuration flussClientConfig;

    private DataStream<MultiplexCdcRecord> input = null;
    private double committerCpu = 1;
    @Nullable private MemorySize committerMemory;

    public PaimonDataBaseSyncSinkBuilder(
            Map<String, String> catalogConfig, Configuration flussClientConfig) {
        this.catalogOptions = Options.fromMap(catalogConfig);
        this.flussClientConfig = flussClientConfig;
    }

    public PaimonDataBaseSyncSinkBuilder withInput(DataStream<MultiplexCdcRecord> input) {
        this.input = input;
        return this;
    }

    public PaimonDataBaseSyncSinkBuilder withCommitterCpu(double committerCpu) {
        this.committerCpu = committerCpu;
        return this;
    }

    public PaimonDataBaseSyncSinkBuilder withCommitterMemory(MemorySize committerMemory) {
        this.committerMemory = committerMemory;
        return this;
    }

    public void build() {
        checkNotNull(input, "input data stream must be not null");
        DataStream<MultiplexCdcRecord> partitioned =
                // we must partition the input stream by bucket,
                // to make sure same bucket shuffle to the same task as
                // paimon don't support write same bucket by different tasks
                input.partitionCustom(
                        (tableBucket, numPartitions) -> {
                            // we first calculate a startIndex by table id and partition id and than
                            // hash by the startIndex and bucket
                            // to make sure the buckets in different table can uniformly
                            // distributed across the subtasks
                            int startIndex =
                                    ((Objects.hash(
                                                                    tableBucket.getTableId(),
                                                                    tableBucket.getPartitionId())
                                                            * 31)
                                                    & 0x7FFFFFFF)
                                            % numPartitions;
                            return (startIndex + tableBucket.getBucket()) % numPartitions;
                        },
                        MultiplexCdcRecord::getTableBucket);

        PaimonMultiTableSink paimonMultiTableSink =
                new PaimonMultiTableSink(
                        catalogLoader(), flussClientConfig, committerCpu, committerMemory);

        paimonMultiTableSink.sinkFrom(partitioned);
    }

    private CatalogLoader catalogLoader() {
        // to make the workflow serializable
        Options catalogOptions = this.catalogOptions;
        return () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
    }
}
