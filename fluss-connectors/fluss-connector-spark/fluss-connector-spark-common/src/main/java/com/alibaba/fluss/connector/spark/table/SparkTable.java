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

package com.alibaba.fluss.connector.spark.table;

import com.alibaba.fluss.connector.spark.utils.SparkConversions;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** spark table. */
public class SparkTable implements Table {

    private final TablePath tablePath;
    private final TableDescriptor tableDescriptor;
    private final StructType sparkSchema;
    private final Transform[] transforms;
    private final Map<String, String> properties;
    private final String bootstrapServers;

    public SparkTable(
            TablePath tablePath, TableDescriptor tableDescriptor, String bootstrapServers) {
        this.tablePath = tablePath;
        this.tableDescriptor = tableDescriptor;
        this.sparkSchema = SparkConversions.toSparkSchema(tableDescriptor.getSchema());
        this.transforms = SparkConversions.toSparkTransforms(tableDescriptor.getPartitionKeys());
        this.properties = new HashMap<>();
        this.properties.putAll(tableDescriptor.getProperties());
        this.properties.putAll(tableDescriptor.getCustomProperties());
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public Transform[] partitioning() {
        return transforms;
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    @Override
    public String name() {
        return tablePath.getTableName();
    }

    @Override
    public StructType schema() {
        return sparkSchema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Collections.emptySet();
    }
}
