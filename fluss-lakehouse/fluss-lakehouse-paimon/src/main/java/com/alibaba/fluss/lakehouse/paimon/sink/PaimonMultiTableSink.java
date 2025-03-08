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
import com.alibaba.fluss.lakehouse.paimon.sink.committable.PaimonMultiTableCommittableTypeInfo;
import com.alibaba.fluss.lakehouse.paimon.sink.committable.PaimonWrappedManifestCommittableSerializer;
import com.alibaba.fluss.lakehouse.paimon.sink.committable.PaimonWrapperManifestCommittable;
import com.alibaba.fluss.lakehouse.paimon.sink.committer.FlussLakeTableSnapshotCommitter;
import com.alibaba.fluss.lakehouse.paimon.sink.committer.PaimonStoreMultiCommitter;
import com.alibaba.fluss.lakehouse.paimon.sink.operator.PaimonMultiWriterOperatorFactory;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.flink.sink.CommittableStateManager;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.CommitterOperatorFactory;
import org.apache.paimon.flink.sink.FlinkSink;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.MultiTableCommittableChannelComputer;
import org.apache.paimon.flink.sink.RestoreAndFailCommittableStateManager;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.flink.sink.WrappedManifestCommittableSerializer;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.UUID;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** The paimon sink to write multi-tables, most copied from Paimon's FlinkCdcMultiTableSink. */
public class PaimonMultiTableSink implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String WRITER_NAME = "MultiplexWriter";
    private static final String GLOBAL_COMMITTER_NAME = "Multiplex Global Committer";

    private final CatalogLoader catalogLoader;

    private final double commitCpuCores;
    @Nullable private final MemorySize commitHeapMemory;
    private final Configuration flussClientConf;

    public PaimonMultiTableSink(
            CatalogLoader catalogLoader,
            Configuration flussClientConf,
            double commitCpuCores,
            @Nullable MemorySize commitHeapMemory) {
        this.catalogLoader = catalogLoader;
        this.flussClientConf = flussClientConf;
        this.commitCpuCores = commitCpuCores;
        this.commitHeapMemory = commitHeapMemory;
    }

    private StoreSinkWrite.WithWriteBufferProvider createWriteProvider() {
        // for now, no compaction for multiplexed sink
        return (table, commitUser, state, ioManager, memoryPoolFactory, metricGroup) ->
                new StoreSinkWriteImpl(
                        table,
                        commitUser,
                        state,
                        ioManager,
                        false,
                        table.coreOptions().prepareCommitWaitCompaction(),
                        true,
                        memoryPoolFactory,
                        metricGroup);
    }

    public DataStreamSink<?> sinkFrom(DataStream<MultiplexCdcRecord> input) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        String initialCommitUser = UUID.randomUUID().toString();
        return sinkFrom(input, initialCommitUser, createWriteProvider());
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<MultiplexCdcRecord> input,
            String commitUser,
            StoreSinkWrite.WithWriteBufferProvider sinkProvider) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        FlinkSink.assertStreamingConfiguration(env);

        PaimonMultiTableCommittableTypeInfo typeInfo = new PaimonMultiTableCommittableTypeInfo();
        SingleOutputStreamOperator<MultiTableCommittable> written =
                input.transform(
                                WRITER_NAME,
                                typeInfo,
                                createWriteOperatorFactory(sinkProvider, commitUser))
                        .setParallelism(input.getParallelism());

        // shuffle committables by table
        DataStream<MultiTableCommittable> partitioned =
                FlinkStreamPartitioner.partition(
                        written,
                        new MultiTableCommittableChannelComputer(),
                        input.getParallelism());

        SingleOutputStreamOperator<?> committed =
                partitioned
                        .transform(
                                GLOBAL_COMMITTER_NAME,
                                typeInfo,
                                new CommitterOperatorFactory<>(
                                        true,
                                        false,
                                        commitUser,
                                        createCommitterFactory(),
                                        createCommittableStateManager()))
                        .setParallelism(input.getParallelism());
        FlinkSink.configureGlobalCommitter(committed, commitCpuCores, commitHeapMemory);
        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    protected OneInputStreamOperatorFactory<MultiplexCdcRecord, MultiTableCommittable>
            createWriteOperatorFactory(
                    StoreSinkWrite.WithWriteBufferProvider writeProvider, String commitUser) {
        return new PaimonMultiWriterOperatorFactory(
                catalogLoader, writeProvider, commitUser, new Options());
    }

    // Table committers are dynamically created at runtime
    private Committer.Factory<MultiTableCommittable, PaimonWrapperManifestCommittable>
            createCommitterFactory() {
        // If checkpoint is enabled for streaming job, we have to
        // commit new files list even if they're empty.
        // Otherwise we can't tell if the commit is successful after
        // a restart.
        return context ->
                new PaimonStoreMultiCommitter(
                        catalogLoader,
                        context,
                        new FlussLakeTableSnapshotCommitter(
                                flussClientConf, context.metricGroup()));
    }

    protected CommittableStateManager<PaimonWrapperManifestCommittable>
            createCommittableStateManager() {
        return new RestoreAndFailCommittableStateManager<>(
                () ->
                        new PaimonWrappedManifestCommittableSerializer(
                                new WrappedManifestCommittableSerializer()));
    }
}
