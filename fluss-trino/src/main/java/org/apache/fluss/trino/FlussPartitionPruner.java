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

import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.types.DataTypeRoot;

import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static org.apache.fluss.trino.FlussPredicateValues.toFlussValue;
import static org.apache.fluss.utils.PartitionUtils.convertValueOfType;
import static org.apache.fluss.utils.PartitionUtils.parseValueOfType;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Restricts Admin discovery by equality and evaluates exact domains on returned partitions. */
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
            // Binary partition-name conversion is not enabled by this connector yet.
            if (typeRoot == DataTypeRoot.BYTES) {
                continue;
            }
            toFlussValue(typeRoot, domain.getType(), domain.getSingleValue())
                    .map(value -> convertValueOfType(value, typeRoot))
                    .ifPresent(value -> partitionValues.put(columnName, value));
        }
        return partitionValues.isEmpty()
                ? Optional.empty()
                : Optional.of(new PartitionSpec(partitionValues));
    }

    static List<PartitionInfo> prunePartitions(
            TableInfo tableInfo,
            TupleDomain<FlussColumnHandle> constraint,
            List<PartitionInfo> partitions) {
        if (constraint.isNone()) {
            return Collections.emptyList();
        }

        List<PartitionDomain> filters = new ArrayList<>();
        Map<FlussColumnHandle, Domain> domains = constraint.getDomains().get();
        for (String name : tableInfo.getPartitionKeys()) {
            int ordinal = tableInfo.getRowType().getFieldIndex(name);
            Schema.Column column = tableInfo.getSchema().getColumns().get(ordinal);
            Domain domain = domains.get(new FlussColumnHandle(name, ordinal));
            if (domain == null || domain.isNullAllowed() || domain.getValues().isAll()) {
                continue;
            }
            DataTypeRoot root = column.getDataType().getTypeRoot();
            switch (root) {
                case BOOLEAN:
                case STRING:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case DATE:
                    if (domain.getType()
                            .equals(FlussTypeConverter.toTrinoType(column.getDataType()))) {
                        filters.add(new PartitionDomain(name, root, domain));
                    }
                    break;
                default:
                    break;
            }
        }

        if (filters.isEmpty()) {
            return partitions;
        }
        List<PartitionInfo> selected = new ArrayList<>();
        for (PartitionInfo partition : partitions) {
            Map<String, String> values = partition.getPartitionSpec().getSpecMap();
            boolean matches = true;
            for (PartitionDomain filter : filters) {
                if (!filter.mayMatch(values.get(filter.name))) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                selected.add(partition);
            }
        }
        return selected;
    }

    private static final class PartitionDomain {
        private final String name;
        private final DataTypeRoot root;
        private final Domain domain;

        private PartitionDomain(String name, DataTypeRoot root, Domain domain) {
            this.name = name;
            this.root = root;
            this.domain = domain;
        }

        private boolean mayMatch(String value) {
            if (value == null) {
                return true;
            }
            if (root == DataTypeRoot.STRING) {
                Slice utf8 = utf8Slice(value);
                return !value.equals(utf8.toStringUtf8()) || domain.includesNullableValue(utf8);
            }
            try {
                Object parsed = parseValueOfType(value, root);
                // The DATE parser narrows epoch days to int, so verify the original encoding.
                if (!value.equals(convertValueOfType(parsed, root))) {
                    return true;
                }
                Object nativeValue =
                        parsed instanceof Number ? ((Number) parsed).longValue() : parsed;
                return domain.includesNullableValue(nativeValue);
            } catch (IllegalArgumentException | DateTimeException | IndexOutOfBoundsException e) {
                return true;
            }
        }
    }
}
