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

import org.apache.fluss.client.table.BucketCalculator;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.GenericRow;

import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.trino.FlussErrorCode.FLUSS_SPLIT_ERROR;
import static org.apache.fluss.trino.FlussPredicateValues.toFlussValue;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Extracts safe equality predicates for physical bucket pruning. */
final class FlussBucketPruner {

    private final BucketCalculator calculator;
    private final byte[] encodedKey;

    private FlussBucketPruner(BucketCalculator calculator, byte[] encodedKey) {
        this.calculator = checkNotNull(calculator, "calculator is null");
        this.encodedKey = checkNotNull(encodedKey, "encodedKey is null");
    }

    /** Returns empty when a complete and safely encodable bucket key cannot be obtained. */
    static Optional<FlussBucketPruner> create(
            TableInfo tableInfo, TupleDomain<FlussColumnHandle> constraint) {
        checkNotNull(tableInfo, "tableInfo is null");
        checkNotNull(constraint, "constraint is null");

        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);

        // Keep conservative fallback for unsupported lake formats.
        if (lakeFormat == DataLakeFormat.HUDI
                || (lakeFormat == DataLakeFormat.ICEBERG
                        && tableInfo.getBucketKeys().size() != 1)) {
            return Optional.empty();
        }

        if (!tableInfo.hasBucketKey() || constraint.isAll() || constraint.isNone()) {
            return Optional.empty();
        }

        List<String> bucketKeys = tableInfo.getBucketKeys();
        Map<FlussColumnHandle, Domain> domains = constraint.getDomains().get();
        List<Schema.Column> columns = tableInfo.getSchema().getColumns();

        GenericRow row = new GenericRow(bucketKeys.size());

        for (int i = 0; i < bucketKeys.size(); i++) {
            String name = bucketKeys.get(i);
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

        BucketCalculator calculator = BucketCalculator.forBucketKeyRow(tableInfo);
        byte[] encodedKey = calculator.encodeKey(row);

        return Optional.of(new FlussBucketPruner(calculator, encodedKey));
    }

    int getBucketId(int bucketCount) {
        if (bucketCount <= 0) {
            throw new TrinoException(FLUSS_SPLIT_ERROR, "Invalid bucket count: " + bucketCount);
        }
        try {
            return calculator.getBucketId(encodedKey, bucketCount);
        } catch (IllegalArgumentException e) {
            throw new TrinoException(FLUSS_SPLIT_ERROR, e.getMessage(), e);
        }
    }
}
