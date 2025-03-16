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

package com.alibaba.fluss.lakehouse.paimon;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.paimon.record.MultiplexCdcRecord;
import com.alibaba.fluss.lakehouse.paimon.sink.NewTablesAddedPaimonListener;
import com.alibaba.fluss.lakehouse.paimon.sink.PaimonDataBaseSyncSinkBuilder;
import com.alibaba.fluss.lakehouse.paimon.source.Filter;
import com.alibaba.fluss.lakehouse.paimon.source.FlussDatabaseSyncSource;
import com.alibaba.fluss.metadata.TableInfo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.regex.Pattern;

import static com.alibaba.fluss.utils.PropertiesUtils.extractAndRemovePrefix;

/** The entrypoint for fluss tier data to Paimon. */
public class FlussLakehousePaimon {

    // for fluss source config
    private static final String DATABASE = "database";
    private static final String FLUSS_CONF_PREFIX = "fluss.";
    // for paimon config
    private static final String PAIMON_CONF_PREFIX = "datalake.paimon.";

    public static void main(String[] args) throws Exception {
        // parse params
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        Map<String, String> paramsMap = params.toMap();

        // the database to sync
        String database = paramsMap.get(DATABASE);

        // extract fluss config
        Map<String, String> flussConfigMap = extractAndRemovePrefix(paramsMap, FLUSS_CONF_PREFIX);
        // we need to get bootstrap.servers
        String bootstrapServers = paramsMap.get(ConfigOptions.BOOTSTRAP_SERVERS.key());
        if (bootstrapServers == null) {
            throw new IllegalArgumentException("bootstrap.servers is not configured");
        }
        flussConfigMap.put(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);

        // extract paimon config
        Map<String, String> paimonConfig = extractAndRemovePrefix(paramsMap, PAIMON_CONF_PREFIX);

        // then build the fluss to paimon job
        final StreamExecutionEnvironment execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();

        Filter<TableInfo> tableFilter =
                (tableInfo) ->
                        // data lake should enable
                        tableInfo.getTableConfig().isDataLakeEnabled();
        Configuration flussConfig = Configuration.fromMap(flussConfigMap);
        FlussDatabaseSyncSource.Builder databaseSyncSourceBuilder =
                FlussDatabaseSyncSource.newBuilder(flussConfig)
                        .withTableFilter(tableFilter)
                        .withNewTableAddedListener(
                                new NewTablesAddedPaimonListener(
                                        Configuration.fromMap(paimonConfig)));
        if (database != null) {
            Filter<String> databaseFilter = new DatabaseFilter(database);
            databaseSyncSourceBuilder.withDatabaseFilter(databaseFilter);
        }

        DataStreamSource<MultiplexCdcRecord> input =
                execEnv.fromSource(
                        databaseSyncSourceBuilder.build(),
                        WatermarkStrategy.noWatermarks(),
                        "FlussSource");

        PaimonDataBaseSyncSinkBuilder paimonDataBaseSyncSinkBuilder =
                new PaimonDataBaseSyncSinkBuilder(paimonConfig, flussConfig).withInput(input);
        paimonDataBaseSyncSinkBuilder.build();

        System.out.println("Starting data tiering service to Paimon.....");
        execEnv.executeAsync();
    }

    private static class DatabaseFilter implements Filter<String> {
        private static final long serialVersionUID = 1L;
        private final Pattern databasePattern;

        public DatabaseFilter(String database) {
            this.databasePattern = Pattern.compile(database);
        }

        @Override
        public boolean test(String database) {
            return databasePattern.matcher(database).matches();
        }
    }
}
