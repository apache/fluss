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

package com.alibaba.fluss.connector.spark.utils;

import com.alibaba.fluss.config.ConfigOption;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.FlussConfigUtils;
import com.alibaba.fluss.connector.spark.SparkConnectorOptions;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;

import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.connector.spark.SparkConnectorOptions.BUCKET_KEY;
import static com.alibaba.fluss.connector.spark.SparkConnectorOptions.BUCKET_NUMBER;
import static com.alibaba.fluss.connector.spark.SparkConnectorOptions.PRIMARY_KEY;

/** Utils for conversion between Spark and Fluss. */
public class SparkConversions {

    /** Convert Spark's table to Fluss's table. */
    public static TableDescriptor toFlussTable(
            StructType sparkSchema, Transform[] partitions, Map<String, String> properties) {
        // schema
        Schema.Builder schemBuilder = Schema.newBuilder();

        if (properties.containsKey(PRIMARY_KEY.key())) {
            List<String> primaryKey =
                    Arrays.stream(properties.get(PRIMARY_KEY.key()).split(","))
                            .map(String::trim)
                            .collect(Collectors.toList());
            schemBuilder.primaryKey(primaryKey);
        }

        Schema schema =
                schemBuilder
                        .fromColumns(
                                Arrays.stream(sparkSchema.fields())
                                        .map(
                                                field ->
                                                        new Schema.Column(
                                                                field.name(),
                                                                SparkTypeUtils.toFlussType(
                                                                                field.dataType())
                                                                        .copy(field.nullable()),
                                                                field.getComment()
                                                                        .getOrElse(() -> null)))
                                        .collect(Collectors.toList()))
                        .build();

        // partition keys
        List<String> partitionKeys =
                Arrays.stream(partitions)
                        .map(partition -> partition.references()[0].describe())
                        .collect(Collectors.toList());

        // bucket keys
        List<String> bucketKey;
        if (properties.containsKey(BUCKET_KEY.key())) {
            bucketKey =
                    Arrays.stream(properties.get(BUCKET_KEY.key()).split(","))
                            .map(String::trim)
                            .collect(Collectors.toList());
        } else {
            // use primary keys - partition keys
            bucketKey =
                    schema.getPrimaryKey()
                            .map(
                                    pk -> {
                                        List<String> bucketKeys =
                                                new ArrayList<>(pk.getColumnNames());
                                        bucketKeys.removeAll(partitionKeys);
                                        return bucketKeys;
                                    })
                            .orElse(Collections.emptyList());
        }
        Integer bucketNum = null;
        if (properties.containsKey(BUCKET_NUMBER.key())) {
            bucketNum = Integer.parseInt(properties.get(BUCKET_NUMBER.key()));
        }

        // process properties
        Map<String, String> flussTableProperties =
                convertSparkOptionsToFlussTableProperties(properties);

        // comment
        String comment = properties.get("comment");

        // TODO: process watermark
        return TableDescriptor.builder()
                .schema(schema)
                .partitionedBy(partitionKeys)
                .distributedBy(bucketNum, bucketKey)
                .comment(comment)
                .properties(flussTableProperties)
                .customProperties(properties)
                .build();
    }

    private static Map<String, String> convertSparkOptionsToFlussTableProperties(
            Map<String, String> options) {
        Map<String, String> properties = new HashMap<>();
        for (ConfigOption<?> option : SparkConnectorOptions.TABLE_OPTIONS) {
            if (options.containsKey(option.key())) {
                properties.put(option.key(), options.get(option.key()));
            }
        }
        return properties;
    }

    public static Configuration toFlussClientConfig(CaseInsensitiveStringMap options) {
        Configuration flussConfig = new Configuration();
        flussConfig.setString(
                SparkConnectorOptions.BOOTSTRAP_SERVERS.key(),
                options.get(SparkConnectorOptions.BOOTSTRAP_SERVERS.key()));
        // forward all client configs
        for (ConfigOption<?> option : FlussConfigUtils.CLIENT_OPTIONS.values()) {
            if (options.get(option.key()) != null) {
                flussConfig.setString(option.key(), options.get(option.key()));
            }
        }

        // pass io tmp dir to fluss client.
        flussConfig.setString(
                ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR,
                new File(options.get(SparkConnectorOptions.TMP_DIRS.key()), "/fluss")
                        .getAbsolutePath());
        return flussConfig;
    }
}
