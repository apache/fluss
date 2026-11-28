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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.fluss.trino.FlussErrorCode.FLUSS_SPLIT_ERROR;
import static org.apache.fluss.trino.FlussPredicateValues.toFlussValue;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Routes a bounded set of complete bucket keys through the writer's bucket calculator. */
final class FlussBucketPruner {

    private static final int MAX_BUCKET_KEY_COMBINATIONS = 256;

    private final BucketCalculator calculator;
    private final List<byte[]> encodedKeys;

    private FlussBucketPruner(BucketCalculator calculator, List<byte[]> encodedKeys) {
        this.calculator = checkNotNull(calculator, "calculator is null");
        this.encodedKeys = encodedKeys;
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

        List<List<Object>> keyValues = new ArrayList<>();
        int combinations = 1;

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
            if (domain == null || domain.isNullAllowed() || !domain.getValues().isDiscreteSet()) {
                return Optional.empty();
            }

            List<Object> literals = domain.getValues().getDiscreteSet();
            if (literals.isEmpty()
                    || combinations > MAX_BUCKET_KEY_COMBINATIONS / literals.size()) {
                return Optional.empty();
            }
            combinations *= literals.size();

            List<Object> values = new ArrayList<>(literals.size());
            for (Object literal : literals) {
                Optional<Object> value =
                        toFlussValue(column.getDataType().getTypeRoot(), domain.getType(), literal);
                if (!value.isPresent()) {
                    return Optional.empty();
                }
                values.add(value.get());
            }
            keyValues.add(values);
        }

        BucketCalculator calculator = BucketCalculator.forBucketKeyRow(tableInfo);
        List<byte[]> encodedKeys = new ArrayList<>(combinations);
        GenericRow row = new GenericRow(bucketKeys.size());
        for (int key = 0; key < combinations; key++) {
            // Enumerate the Cartesian product of bucket-key values.
            int remaining = key;
            for (int i = keyValues.size() - 1; i >= 0; i--) {
                List<Object> values = keyValues.get(i);
                row.setField(i, values.get(remaining % values.size()));
                remaining /= values.size();
            }
            encodedKeys.add(calculator.encodeKey(row));
        }
        return Optional.of(new FlussBucketPruner(calculator, encodedKeys));
    }

    List<Integer> getBucketIds(int bucketCount) {
        if (bucketCount <= 0) {
            throw new TrinoException(FLUSS_SPLIT_ERROR, "Invalid bucket count: " + bucketCount);
        }
        try {
            Set<Integer> buckets = new TreeSet<>();
            for (byte[] encodedKey : encodedKeys) {
                buckets.add(calculator.getBucketId(encodedKey, bucketCount));
            }
            return new ArrayList<>(buckets);
        } catch (IllegalArgumentException e) {
            throw new TrinoException(FLUSS_SPLIT_ERROR, e.getMessage(), e);
        }
    }
}
