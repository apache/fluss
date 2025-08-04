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

package com.alibaba.fluss.lake.lance;

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
    private static final String index_cache_size = "index_cache_size";
    private static final String metadata_cache_size = "metadata_cache_size";
    private static final String max_row_per_file = "max_row_per_file";
    private static final String max_rows_per_group = "max_rows_per_group";
    private static final String max_bytes_per_file = "max_bytes_per_file";
    private static final String ak = "access_key_id";
    private static final String sk = "secret_access_key";
    private static final String endpoint = "aws_endpoint";
    private static final String region = "aws_region";
    private static final String virtual_hosted_style = "virtual_hosted_style_request";
    private static final String allow_http = "allow_http";
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
            Map<String, String> properties, String databaseName, String tableName) {
        if (!properties.containsKey(warehouse)) {
            throw new IllegalArgumentException("Missing required option " + warehouse);
        }
        return new LanceConfig(databaseName, tableName, properties.get(warehouse), properties);
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

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
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
        if (maps.containsKey(index_cache_size)) {
            builder.setIndexCacheSize(Integer.parseInt(maps.get(index_cache_size)));
        }
        if (maps.containsKey(metadata_cache_size)) {
            builder.setMetadataCacheSize(Integer.parseInt(maps.get(metadata_cache_size)));
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

    private static Map<String, String> genStorageOptions(LanceConfig config) {
        Map<String, String> storageOptions = new HashMap<>();
        Map<String, String> maps = config.getOptions();
        if (maps.containsKey(ak) && maps.containsKey(sk) && maps.containsKey(endpoint)) {
            storageOptions.put(ak, maps.get(ak));
            storageOptions.put(sk, maps.get(sk));
            storageOptions.put(endpoint, maps.get(endpoint));
        }
        if (maps.containsKey(region)) {
            storageOptions.put(region, maps.get(region));
        }
        if (maps.containsKey(virtual_hosted_style)) {
            storageOptions.put(virtual_hosted_style, maps.get(virtual_hosted_style));
        }
        if (maps.containsKey(allow_http)) {
            storageOptions.put(allow_http, maps.get(allow_http));
        }
        return storageOptions;
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
