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

package org.apache.fluss.spark.utils;

import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.FlussConfigUtils;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.ValidationException;
import org.apache.fluss.spark.SparkConnectorOptions;

import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utils for conversion between Spark and Fluss. */
public class SparkConversions {
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** Convert Spark's table to Fluss's table. */
    public static TableDescriptor toFlussTable(
            StructType sparkSchema, Transform[] partitions, Map<String, String> properties) {
        // schema
        Schema.Builder schemBuilder = Schema.newBuilder();

        if (properties.containsKey(SparkConnectorOptions.PRIMARY_KEY.key())) {
            List<String> primaryKey =
                    Arrays.stream(
                                    properties
                                            .get(SparkConnectorOptions.PRIMARY_KEY.key())
                                            .split(","))
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
        if (properties.containsKey(SparkConnectorOptions.BUCKET_KEY.key())) {
            bucketKey =
                    Arrays.stream(properties.get(SparkConnectorOptions.BUCKET_KEY.key()).split(","))
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
        if (properties.containsKey(SparkConnectorOptions.BUCKET_NUMBER.key())) {
            bucketNum = Integer.parseInt(properties.get(SparkConnectorOptions.BUCKET_NUMBER.key()));
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

        String tmpDir = flussConfig.get(SparkConnectorOptions.TMP_DIRS);
        // pass io tmp dir to fluss client.
        flussConfig.setString(
                ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR,
                new File(tmpDir, "/fluss").getAbsolutePath());
        return flussConfig;
    }

    public static Configuration toFlussConfig(CaseInsensitiveStringMap options) {
        Configuration flussConfig = new Configuration();
        flussConfig.setString(
                SparkConnectorOptions.BOOTSTRAP_SERVERS.key(),
                options.get(SparkConnectorOptions.BOOTSTRAP_SERVERS.key()));
        // forward all configs
        options.forEach(flussConfig::setString);

        String tmpDir = flussConfig.get(SparkConnectorOptions.TMP_DIRS);
        // pass io tmp dir to fluss client.
        flussConfig.setString(
                ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR,
                new File(tmpDir, "/fluss").getAbsolutePath());
        return flussConfig;
    }

    /**
     * Parses timestamp String to Long.
     *
     * <p>timestamp String format was given as following:
     *
     * <pre>
     *     scan.startup.timestamp = 1678883047356
     *     scan.startup.timestamp = 2023-12-09 23:09:12
     * </pre>
     *
     * @return timestamp as long value
     */
    public static long parseTimestamp(String timestampStr, String optionKey, ZoneId timeZone) {
        if (timestampStr.matches("\\d+")) {
            return Long.parseLong(timestampStr);
        }

        try {
            return LocalDateTime.parse(timestampStr, DATE_TIME_FORMATTER)
                    .atZone(timeZone)
                    .toInstant()
                    .toEpochMilli();
        } catch (Exception e) {
            throw new ValidationException(
                    String.format(
                            "Invalid properties '%s' should follow the format "
                                    + "'yyyy-MM-dd HH:mm:ss' or 'timestamp', but is '%s'. "
                                    + "You can config like: '2023-12-09 23:09:12' or '1678883047356'.",
                            optionKey, timestampStr),
                    e);
        }
    }
}
