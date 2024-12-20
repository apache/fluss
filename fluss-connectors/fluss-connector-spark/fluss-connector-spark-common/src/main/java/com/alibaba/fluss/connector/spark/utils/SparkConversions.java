/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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
import com.alibaba.fluss.connector.spark.SparkConnectorOptions;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.DoubleType;
import com.alibaba.fluss.types.FloatType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.SmallIntType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.types.TimestampType;
import com.alibaba.fluss.types.TinyIntType;

import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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
                                                                SparkConversions.toFlussType(field),
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

    /** Convert Fluss's schema to Spark's schema. */
    public static StructType toSparkSchema(Schema flussSchema) {
        StructField[] fields = new StructField[flussSchema.getColumns().size()];
        for (int i = 0; i < flussSchema.getColumns().size(); i++) {
            fields[i] = toSparkStructField(flussSchema.getColumns().get(i));
        }
        return new StructType(fields);
    }

    /** Convert Fluss's partition keys to Spark's transforms. */
    public static Transform[] toSparkTransforms(List<String> partitionKeys) {
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return new Transform[0];
        }
        Transform[] transforms = new Transform[partitionKeys.size()];
        for (int i = 0; i < partitionKeys.size(); i++) {
            transforms[i] = Expressions.identity(partitionKeys.get(i));
        }
        return transforms;
    }

    /** Convert Fluss's column to Spark's field. */
    public static StructField toSparkStructField(Schema.Column flussColumn) {
        StructField field =
                new StructField(
                        flussColumn.getName(),
                        toSparkType(flussColumn.getDataType()),
                        flussColumn.getDataType().isNullable(),
                        Metadata.empty());
        return flussColumn.getComment().isPresent()
                ? field.withComment(flussColumn.getComment().get())
                : field;
    }

    /** Convert Fluss's type to Spark's type. */
    public static org.apache.spark.sql.types.DataType toSparkType(DataType flussDataType) {
        return flussDataType.accept(FlussTypeToSparkType.INSTANCE);
    }

    /** Convert Spark's type to Fluss's type. */
    public static DataType toFlussType(StructField sparkField) {
        org.apache.spark.sql.types.DataType sparkType = sparkField.dataType();
        boolean isNullable = sparkField.nullable();
        if (sparkType instanceof org.apache.spark.sql.types.CharType) {
            return new CharType(
                    isNullable, ((org.apache.spark.sql.types.CharType) sparkType).length());
        } else if (sparkType instanceof org.apache.spark.sql.types.StringType) {
            return new StringType(isNullable);
        } else if (sparkType instanceof org.apache.spark.sql.types.BooleanType) {
            return new BooleanType(isNullable);
        } else if (sparkType instanceof org.apache.spark.sql.types.BinaryType) {
            return new BytesType(isNullable);
        } else if (sparkType instanceof org.apache.spark.sql.types.DecimalType) {
            return new DecimalType(
                    isNullable,
                    ((org.apache.spark.sql.types.DecimalType) sparkType).precision(),
                    ((org.apache.spark.sql.types.DecimalType) sparkType).scale());
        } else if (sparkType instanceof org.apache.spark.sql.types.ByteType) {
            return new TinyIntType(isNullable);
        } else if (sparkType instanceof org.apache.spark.sql.types.ShortType) {
            return new SmallIntType(isNullable);
        } else if (sparkType instanceof org.apache.spark.sql.types.IntegerType) {
            return new IntType(isNullable);
        } else if (sparkType instanceof org.apache.spark.sql.types.LongType) {
            return new BigIntType(isNullable);
        } else if (sparkType instanceof org.apache.spark.sql.types.FloatType) {
            return new FloatType(isNullable);
        } else if (sparkType instanceof org.apache.spark.sql.types.DoubleType) {
            return new DoubleType(isNullable);
        } else if (sparkType instanceof org.apache.spark.sql.types.DateType) {
            return new DateType(isNullable);
        } else if (sparkType instanceof org.apache.spark.sql.types.TimestampType) {
            // spark only support 6 digits of precision
            return new LocalZonedTimestampType(isNullable, 6);
        } else if (sparkType instanceof org.apache.spark.sql.types.TimestampNTZType) {
            // spark only support 6 digits of precision
            return new TimestampType(isNullable, 6);
        } else {
            // TODO: support more data type
            throw new UnsupportedOperationException("Unsupported data type: " + sparkType);
        }
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
}
