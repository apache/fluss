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

package org.apache.fluss.flink.utils;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;

/** Validator for Flink schema constraints. */
public class FlinkSchemaValidator {

    private FlinkSchemaValidator() {}

    /**
     * Validates that primary key columns do not contain ARRAY type.
     *
     * @param resolvedSchema the resolved schema
     * @param primaryKeyColumns the list of primary key column names
     * @throws CatalogException if a primary key column is not found in schema
     * @throws UnsupportedOperationException if a primary key column is of ARRAY type
     */
    public static void validatePrimaryKeyColumns(
            ResolvedSchema resolvedSchema, List<String> primaryKeyColumns) {
        for (String pkColumn : primaryKeyColumns) {
            Column column =
                    resolvedSchema
                            .getColumn(pkColumn)
                            .orElseThrow(
                                    () ->
                                            new CatalogException(
                                                    "Primary key column "
                                                            + pkColumn
                                                            + " not found in schema."));
            LogicalType logicalType = column.getDataType().getLogicalType();
            if (logicalType instanceof ArrayType) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Column '%s' of ARRAY type is not supported as primary key.",
                                pkColumn));
            }
        }
    }

    /**
     * Validates that partition key columns do not contain ARRAY type.
     *
     * @param resolvedSchema the resolved schema
     * @param partitionKeyColumns the list of partition key column names
     * @throws CatalogException if a partition key column is not found in schema
     * @throws UnsupportedOperationException if a partition key column is of ARRAY type
     */
    public static void validatePartitionKeyColumns(
            ResolvedSchema resolvedSchema, List<String> partitionKeyColumns) {
        for (String partitionKey : partitionKeyColumns) {
            Column column =
                    resolvedSchema
                            .getColumn(partitionKey)
                            .orElseThrow(
                                    () ->
                                            new CatalogException(
                                                    "Partition key column "
                                                            + partitionKey
                                                            + " not found in schema."));
            LogicalType logicalType = column.getDataType().getLogicalType();
            if (logicalType instanceof ArrayType) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Column '%s' of ARRAY type is not supported as partition key.",
                                partitionKey));
            }
        }
    }

    /**
     * Validates that bucket key columns do not contain ARRAY type.
     *
     * @param resolvedSchema the resolved schema
     * @param bucketKeyColumns the list of bucket key column names
     * @throws CatalogException if a bucket key column is not found in schema
     * @throws UnsupportedOperationException if a bucket key column is of ARRAY type
     */
    public static void validateBucketKeyColumns(
            ResolvedSchema resolvedSchema, List<String> bucketKeyColumns) {
        for (String bkColumn : bucketKeyColumns) {
            Column column =
                    resolvedSchema
                            .getColumn(bkColumn)
                            .orElseThrow(
                                    () ->
                                            new CatalogException(
                                                    "Bucket key column "
                                                            + bkColumn
                                                            + " not found in schema."));
            LogicalType logicalType = column.getDataType().getLogicalType();
            if (logicalType instanceof ArrayType) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Column '%s' of ARRAY type is not supported as bucket key.",
                                bkColumn));
            }
        }
    }
}
