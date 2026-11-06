/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataTypeRoot;

import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.fluss.utils.PartitionUtils.convertValueOfType;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Extracts safe equality restrictions for partition-aware bucket discovery. */
final class FlussPartitionPruner {
    private FlussPartitionPruner() {}

    static Optional<PartitionSpec> extractPartitionSpec(
            TableInfo tableInfo, TupleDomain<FlussColumnHandle> constraint) {
        checkNotNull(tableInfo, "tableInfo is null");
        checkNotNull(constraint, "constraint is null");
        checkArgument(!constraint.isNone(), "constraint is none; skip split planning instead");
        if (!tableInfo.isPartitioned() || constraint.isAll()) {
            return Optional.empty();
        }

        Map<FlussColumnHandle, Domain> domains = constraint.getDomains().get();
        Map<String, String> partitionValues = new LinkedHashMap<>();
        List<Schema.Column> columns = tableInfo.getSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Schema.Column column = columns.get(i);
            String columnName = column.getName();
            if (!tableInfo.getPartitionKeys().contains(columnName)) {
                continue;
            }

            Domain domain = domains.get(new FlussColumnHandle(columnName, i));
            if (domain == null || !domain.isSingleValue()) {
                continue;
            }

            DataTypeRoot typeRoot = column.getDataType().getTypeRoot();
            toFlussValue(typeRoot, domain)
                    .map(value -> convertValueOfType(value, typeRoot))
                    .ifPresent(value -> partitionValues.put(columnName, value));
        }
        return partitionValues.isEmpty()
                ? Optional.empty()
                : Optional.of(new PartitionSpec(partitionValues));
    }

    private static Optional<Object> toFlussValue(DataTypeRoot typeRoot, Domain domain) {
        switch (typeRoot) {
            case STRING:
                return singleValue(domain, VARCHAR)
                        .map(Slice.class::cast)
                        .map(value -> BinaryString.fromString(value.toStringUtf8()));
            case BOOLEAN:
                return singleValue(domain, BOOLEAN);
            case TINYINT:
                return singleValue(domain, TINYINT).map(Long.class::cast).map(Long::byteValue);
            case SMALLINT:
                return singleValue(domain, SMALLINT).map(Long.class::cast).map(Long::shortValue);
            case INTEGER:
                return singleValue(domain, INTEGER).map(Long.class::cast).map(Long::intValue);
            case BIGINT:
                return singleValue(domain, BIGINT);
            case DATE:
                return singleValue(domain, DATE).map(Long.class::cast).map(Long::intValue);
            default:
                return Optional.empty();
        }
    }

    private static Optional<Object> singleValue(Domain domain, Type expectedType) {
        return domain.getType().equals(expectedType)
                ? Optional.of(domain.getSingleValue())
                : Optional.empty();
    }
}
