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

package com.alibaba.fluss.lake.iceberg.utils;

import com.alibaba.fluss.metadata.TablePath;

import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import javax.annotation.Nullable;

import static com.alibaba.fluss.metadata.ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR;

/** Utility class for static conversions between Fluss and Iceberg types. */
public class IcebergConversions {

    /** Convert Fluss TablePath to Iceberg TableIdentifier. */
    public static TableIdentifier toIceberg(TablePath tablePath) {
        return TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    public static PartitionKey toPartition(
            Table table, @Nullable String partitionName, int bucket) {
        PartitionSpec partitionSpec = table.spec();
        Schema schema = table.schema();
        PartitionKey partitionKey = new PartitionKey(partitionSpec, schema);
        int pos = 0;
        if (partitionName != null) {
            String[] partitionArr = partitionName.split("\\" + PARTITION_SPEC_SEPARATOR);
            for (String partition : partitionArr) {
                partitionKey.set(pos++, partition);
            }
        }
        partitionKey.set(pos, bucket);
        return partitionKey;
    }
}
