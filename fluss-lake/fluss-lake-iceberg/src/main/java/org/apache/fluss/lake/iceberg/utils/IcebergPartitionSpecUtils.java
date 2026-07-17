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

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.TableDescriptor;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import java.util.List;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.iceberg.types.Type.TypeID.STRING;

/** Utilities for constructing the Iceberg partition spec used by Fluss lake tiering. */
@Internal
public final class IcebergPartitionSpecUtils {

    private IcebergPartitionSpecUtils() {}

    /** Creates an Iceberg partition spec from a Fluss table descriptor. */
    public static PartitionSpec createPartitionSpec(
            TableDescriptor tableDescriptor, Schema icebergSchema) {
        int bucketCount =
                tableDescriptor
                        .getTableDistribution()
                        .flatMap(TableDescriptor.TableDistribution::getBucketCount)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Bucket count (bucket.num) must be set"));
        return createPartitionSpec(
                icebergSchema,
                tableDescriptor.hasPrimaryKey(),
                tableDescriptor.getBucketKeys(),
                tableDescriptor.getPartitionKeys(),
                bucketCount);
    }

    private static PartitionSpec createPartitionSpec(
            Schema icebergSchema,
            boolean isPrimaryKeyTable,
            List<String> bucketKeys,
            List<String> partitionKeys,
            int bucketCount) {
        if (bucketKeys.size() > 1) {
            throw new UnsupportedOperationException(
                    "Only one bucket key is supported for Iceberg at the moment");
        }

        if (bucketKeys.isEmpty() && isPrimaryKeyTable) {
            throw new IllegalArgumentException(
                    "Bucket key must be set for primary key Iceberg tables");
        }

        PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
        for (String partitionKey : partitionKeys) {
            if (!icebergSchema.findType(partitionKey).typeId().equals(STRING)) {
                throw new InvalidTableException(
                        String.format(
                                "Partition key only support string type for iceberg currently. Column `%s` is not string type.",
                                partitionKey));
            }
            builder.identity(partitionKey);
        }

        if (bucketKeys.isEmpty()) {
            // __offset and __timestamp are system data columns, but only __bucket is a
            // partition field when the Fluss table has no bucket key.
            builder.identity(BUCKET_COLUMN_NAME);
        } else {
            builder.bucket(bucketKeys.get(0), bucketCount);
        }
        return builder.build();
    }
}
