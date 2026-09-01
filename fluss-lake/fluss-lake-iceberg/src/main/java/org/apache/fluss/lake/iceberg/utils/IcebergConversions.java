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

import org.apache.fluss.lake.iceberg.source.FlussRowAsIcebergRecord;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.fluss.metadata.ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;

/** Utility class for static conversions between Fluss and Iceberg types. */
public class IcebergConversions {

    /** Convert Fluss TablePath to Iceberg TableIdentifier. */
    public static TableIdentifier toIceberg(TablePath tablePath) {
        return TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Nullable
    public static PartitionKey toPartition(
            Table table, @Nullable String partitionName, int bucket) {
        PartitionSpec partitionSpec = table.spec();
        Schema schema = table.schema();
        // FIP-27: an unpartitioned spec (clean bucket-unaware table) has no partition fields.
        // Returning null lets the writer take Iceberg's canonical unpartitioned path
        // (data/file.parquet); a non-null empty PartitionKey would route through the partitioned
        // path and can produce a malformed data//file.parquet location.
        if (partitionSpec.isUnpartitioned()) {
            return null;
        }
        PartitionKey partitionKey = new PartitionKey(partitionSpec, schema);
        int pos = 0;
        if (partitionName != null) {
            String[] partitionArr = partitionName.split("\\" + PARTITION_SPEC_SEPARATOR);
            for (String partition : partitionArr) {
                partitionKey.set(pos++, partition);
            }
        }
        // Set the bucket value only when the trailing partition field is the Fluss bucket field
        // (legacy identity(__bucket) or a bucket(bucketKey) transform). Bucket-unaware partitioned
        // tables (a trailing identity partition column) have no such field.
        List<PartitionField> fields = partitionSpec.fields();
        PartitionField lastField = fields.get(fields.size() - 1);
        if (IcebergPartitionSpecUtils.isFlussBucketField(schema, lastField)) {
            partitionKey.set(pos, bucket);
        }
        return partitionKey;
    }

    public static Expression toFilterExpression(
            Table table, @Nullable String partitionName, int bucket) {
        List<PartitionField> partitionFields = table.spec().fields();
        Expression expression = Expressions.alwaysTrue();
        int partitionIndex = 0;
        if (partitionName != null) {
            String[] partitionArr = partitionName.split("\\" + PARTITION_SPEC_SEPARATOR);
            for (String partition : partitionArr) {
                expression =
                        Expressions.and(
                                expression,
                                Expressions.equal(
                                        table.schema()
                                                .findColumnName(
                                                        partitionFields
                                                                .get(partitionIndex++)
                                                                .sourceId()),
                                        partition));
            }
        }
        // FIP-27: legacy tables carry the __bucket column and are filtered per bucket. Clean
        // tables have no __bucket column, so no bucket-level filter is applied.
        if (table.schema().findField(BUCKET_COLUMN_NAME) != null) {
            expression = Expressions.and(expression, Expressions.equal(BUCKET_COLUMN_NAME, bucket));
        }
        return expression;
    }

    public static Object toIcebergLiteral(
            Types.NestedField icebergField, DataType flussFieldType, Object flussLiteral) {
        InternalRow flussRow = GenericRow.of(flussLiteral);
        FlussRowAsIcebergRecord flussRowAsIcebergRecord =
                new FlussRowAsIcebergRecord(
                        Types.StructType.of(icebergField), RowType.of(flussFieldType), flussRow);
        return flussRowAsIcebergRecord.get(0, icebergField.type().typeId().javaClass());
    }
}
