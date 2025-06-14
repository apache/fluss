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

package com.alibaba.fluss.flink.tiering;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.tiering.committer.CommittableMessageTypeInfo;
import com.alibaba.fluss.flink.tiering.committer.TieringCommitOperatorFactory;
import com.alibaba.fluss.flink.tiering.source.TableBucketWriteResultTypeInfo;
import com.alibaba.fluss.flink.tiering.source.TieringSource;
import com.alibaba.fluss.lake.lakestorage.LakeStorage;
import com.alibaba.fluss.lake.lakestorage.LakeStoragePlugin;
import com.alibaba.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import com.alibaba.fluss.lake.writer.LakeTieringFactory;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.util.Collections;

import static com.alibaba.fluss.flink.tiering.source.TieringSource.TIERING_SOURCE_TRANSFORMATION_UID;
import static com.alibaba.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** The builder to build Flink lake tiering job. */
public class LakeTieringJobBuilder {

    private static final String DEFAULT_TIERING_SERVICE_JOB_NAME = "Fluss Lake Tiering Service";

    private final StreamExecutionEnvironment env;
    private final Configuration flussConfig;
    private final Configuration dataLakeConfig;
    private final String dataLakeFormat;

    private LakeTieringJobBuilder(
            StreamExecutionEnvironment env,
            Configuration flussConfig,
            Configuration dataLakeConfig,
            String dataLakeFormat) {
        this.env = checkNotNull(env);
        this.flussConfig = checkNotNull(flussConfig);
        this.dataLakeConfig = checkNotNull(dataLakeConfig);
        this.dataLakeFormat = checkNotNull(dataLakeFormat);
    }

    public static LakeTieringJobBuilder newBuilder(
            StreamExecutionEnvironment env,
            Configuration flussConfig,
            Configuration dataLakeConfig,
            String dataLakeFormat) {
        return new LakeTieringJobBuilder(env, flussConfig, dataLakeConfig, dataLakeFormat);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public JobClient build() throws Exception {
        // get the lake storage plugin
        LakeStoragePlugin lakeStoragePlugin =
                LakeStoragePluginSetUp.fromConfiguration(
                        Configuration.fromMap(
                                Collections.singletonMap(
                                        ConfigOptions.DATALAKE_FORMAT.key(), dataLakeFormat)),
                        null);
        // create lake storage from configurations
        LakeStorage lakeStorage = checkNotNull(lakeStoragePlugin).createLakeStorage(dataLakeConfig);

        LakeTieringFactory lakeTieringFactory = lakeStorage.createLakeTieringFactory();

        // build tiering source
        TieringSource.Builder<?> tieringSourceBuilder =
                new TieringSource.Builder<>(flussConfig, lakeTieringFactory);
        if (flussConfig.get(POLL_TIERING_TABLE_INTERVAL) != null) {
            tieringSourceBuilder.withPollTieringTableIntervalMs(
                    flussConfig.get(POLL_TIERING_TABLE_INTERVAL).toMillis());
        }
        TieringSource<?> tieringSource = tieringSourceBuilder.build();
        DataStreamSource<?> source =
                env.fromSource(
                        tieringSource,
                        WatermarkStrategy.noWatermarks(),
                        "TieringSource",
                        TableBucketWriteResultTypeInfo.of(
                                () -> lakeTieringFactory.getWriteResultSerializer()));

        source.getTransformation().setUid(TIERING_SOURCE_TRANSFORMATION_UID);

        source.transform(
                        "TieringCommitter",
                        CommittableMessageTypeInfo.of(
                                () -> lakeTieringFactory.getCommitableSerializer()),
                        new TieringCommitOperatorFactory(flussConfig, lakeTieringFactory))
                .setParallelism(1)
                .setMaxParallelism(1)
                .sinkTo(new DiscardingSink());
        String jobName =
                env.getConfiguration()
                        .getOptional(PipelineOptions.NAME)
                        .orElse(DEFAULT_TIERING_SERVICE_JOB_NAME);

        return env.executeAsync(jobName);
    }
}
