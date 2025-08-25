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

package org.apache.fluss.lake.lance;

import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.WriteParams;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Lance Configuration. */
public class LanceConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String LANCE_FILE_SUFFIX = ".lance";

    private static final String block_size = "block_size";
    private static final String version = "version";
    private static final String max_row_per_file = "max_row_per_file";
    private static final String max_rows_per_group = "max_rows_per_group";
    private static final String max_bytes_per_file = "max_bytes_per_file";
    private static final String batch_size = "batch_size";
    private static final String warehouse = "warehouse";

    private final Map<String, String> options;
    private final String databaseName;
    private final String tableName;
    private final String datasetUri;

    public LanceConfig(
            String databaseName,
            String tableName,
            String warehouse,
            Map<String, String> properties) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.options = properties;

        this.datasetUri = warehouse + "/" + databaseName + "/" + tableName + LANCE_FILE_SUFFIX;
    }

    public static LanceConfig from(
            Map<String, String> clusterConf,
            Map<String, String> tableCustomProperties,
            String databaseName,
            String tableName) {
        if (!clusterConf.containsKey(warehouse)) {
            throw new IllegalArgumentException("Missing required option " + warehouse);
        }
        Map<String, String> options = new HashMap<>();
        options.putAll(clusterConf);
        options.putAll(tableCustomProperties);
        return new LanceConfig(databaseName, tableName, clusterConf.get(warehouse), options);
    }

    public static int getBatchSize(LanceConfig config) {
        Map<String, String> options = config.getOptions();
        if (options.containsKey(batch_size)) {
            return Integer.parseInt(options.get(batch_size));
        }
        return 512;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public String getDatasetUri() {
        return datasetUri;
    }

    public static ReadOptions genReadOptionFromConfig(LanceConfig config) {
        ReadOptions.Builder builder = new ReadOptions.Builder();
        Map<String, String> maps = config.getOptions();
        if (maps.containsKey(block_size)) {
            builder.setBlockSize(Integer.parseInt(maps.get(block_size)));
        }
        if (maps.containsKey(version)) {
            builder.setVersion(Integer.parseInt(maps.get(version)));
        }
        builder.setStorageOptions(genStorageOptions(config));
        return builder.build();
    }

    public static WriteParams genWriteParamsFromConfig(LanceConfig config) {
        WriteParams.Builder builder = new WriteParams.Builder();
        Map<String, String> maps = config.getOptions();
        if (maps.containsKey(max_row_per_file)) {
            builder.withMaxRowsPerFile(Integer.parseInt(maps.get(max_row_per_file)));
        }
        if (maps.containsKey(max_rows_per_group)) {
            builder.withMaxRowsPerGroup(Integer.parseInt(maps.get(max_rows_per_group)));
        }
        if (maps.containsKey(max_bytes_per_file)) {
            builder.withMaxBytesPerFile(Long.parseLong(maps.get(max_bytes_per_file)));
        }
        builder.withStorageOptions(genStorageOptions(config));
        return builder.build();
    }

    public static Map<String, String> genStorageOptions(LanceConfig config) {
        return config.getOptions();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LanceConfig config = (LanceConfig) o;
        return Objects.equals(databaseName, config.databaseName)
                && Objects.equals(tableName, config.tableName)
                && Objects.equals(options, config.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, options);
    }
}
