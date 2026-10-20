/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;

import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.trino.FlussErrorCode.FLUSS_SPLIT_ERROR;
import static org.apache.fluss.trino.FlussPredicateValues.toFlussValue;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Uses Fluss's canonical key encoding and bucketing function to prune physical buckets. */
final class FlussBucketPruner {

    private final byte[] encodedKey;
    private final BucketingFunction bucketingFunction;

    private FlussBucketPruner(TableInfo tableInfo, byte[] encodedKey) {
        this.encodedKey = checkNotNull(encodedKey, "encodedKey is null");
        bucketingFunction =
                BucketingFunction.of(tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
    }

    /** Returns empty when a complete and safely encodable bucket key cannot be obtained. */
    static Optional<FlussBucketPruner> create(
            TableInfo tableInfo, TupleDomain<FlussColumnHandle> constraint) {
        checkNotNull(tableInfo, "tableInfo is null");
        checkNotNull(constraint, "constraint is null");

        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
        // ignore hudi and iceberg tables with multiple bucket keys
        if (lakeFormat == DataLakeFormat.HUDI
                || (lakeFormat == DataLakeFormat.ICEBERG
                        && tableInfo.getBucketKeys().size() != 1)) {
            return Optional.empty();
        }

        if (!tableInfo.hasBucketKey() || constraint.isAll() || constraint.isNone()) {
            return Optional.empty();
        }

        boolean defaultPrimaryKey = tableInfo.hasPrimaryKey() && tableInfo.isDefaultBucketKey();
        List<String> inputFields = tableInfo.getBucketKeys();

        Map<FlussColumnHandle, Domain> domains = constraint.getDomains().get();
        List<Schema.Column> columns = tableInfo.getSchema().getColumns();
        GenericRow row = new GenericRow(inputFields.size());

        for (int i = 0; i < inputFields.size(); i++) {
            String name = inputFields.get(i);
            int fieldIndex = tableInfo.getRowType().getFieldIndex(name);
            if (fieldIndex < 0 || fieldIndex >= columns.size()) {
                return Optional.empty();
            }

            Schema.Column column = columns.get(fieldIndex);
            if (!column.getName().equals(name)) {
                return Optional.empty();
            }

            Domain domain = domains.get(new FlussColumnHandle(name, fieldIndex));
            if (domain == null || !domain.isSingleValue()) {
                return Optional.empty();
            }

            Optional<Object> value = toFlussValue(column.getDataType().getTypeRoot(), domain);
            if (!value.isPresent()) {
                return Optional.empty();
            }

            row.setField(i, value.get());
        }

        RowType inputRowType = tableInfo.getRowType().project(inputFields);

        KeyEncoder bucketEncoder;
        if (defaultPrimaryKey) {
            KeyEncoder primaryKeyEncoder =
                    KeyEncoder.ofPrimaryKeyEncoder(
                            inputRowType,
                            tableInfo.getPhysicalPrimaryKeys(),
                            tableInfo.getTableConfig(),
                            true);
            bucketEncoder =
                    KeyEncoder.ofBucketKeyEncoder(
                            inputRowType,
                            tableInfo.getBucketKeys(),
                            tableInfo.getTableConfig(),
                            true,
                            primaryKeyEncoder);
        } else {
            bucketEncoder =
                    KeyEncoder.ofBucketKeyEncoder(
                            inputRowType,
                            tableInfo.getBucketKeys(),
                            tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        }
        byte[] encodedKey = bucketEncoder.encodeKey(row);
        return Optional.of(new FlussBucketPruner(tableInfo, encodedKey));
    }

    int getBucketId(int bucketCount) {
        if (bucketCount <= 0) {
            throw new TrinoException(FLUSS_SPLIT_ERROR, "Invalid bucket count: " + bucketCount);
        }
        int bucketId = bucketingFunction.bucketing(encodedKey, bucketCount);
        if (bucketId < 0 || bucketId >= bucketCount) {
            throw new TrinoException(
                    FLUSS_SPLIT_ERROR,
                    "Invalid bucket ID " + bucketId + " for bucket count " + bucketCount);
        }
        return bucketId;
    }
}
