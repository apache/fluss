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

import com.alibaba.fluss.connector.spark.SparkConnectorOptions;
import com.alibaba.fluss.metadata.TableDescriptor;

import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SparkConversions}. */
public class SparkConversionsTest {

    @Test
    void testTableConversion() {
        StructField[] sparkColumns =
                new StructField[] {
                    new StructField(
                            "order_id",
                            org.apache.spark.sql.types.DataTypes.LongType,
                            false,
                            Metadata.empty()),
                    new StructField(
                            "order_name",
                            org.apache.spark.sql.types.DataTypes.StringType,
                            true,
                            Metadata.empty())
                };

        // test convert spark table to fluss table
        StructType structType = new StructType(sparkColumns);
        Transform[] transforms = new Transform[0];
        Map<String, String> properties = new HashMap<>();
        properties.put(SparkConnectorOptions.PRIMARY_KEY.key(), "order_id");
        properties.put("comment", "test comment");
        properties.put("k1", "v1");
        properties.put("k2", "v2");
        TableDescriptor flussTable =
                SparkConversions.toFlussTable(structType, transforms, properties);

        String expectFlussTableString =
                "TableDescriptor{schema=("
                        + "order_id BIGINT NOT NULL,"
                        + "order_name STRING,"
                        + "CONSTRAINT PK_order_id PRIMARY KEY (order_id)"
                        + "), comment='test comment', partitionKeys=[], "
                        + "tableDistribution={bucketKeys=[order_id] bucketCount=null}, "
                        + "properties={}, "
                        + "customProperties={comment=test comment, primary.key=order_id, k1=v1, k2=v2}"
                        + "}";
        assertThat(flussTable.toString()).isEqualTo(expectFlussTableString);
    }
}
