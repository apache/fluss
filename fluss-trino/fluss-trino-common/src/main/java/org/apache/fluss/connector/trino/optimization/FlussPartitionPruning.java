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

package org.apache.fluss.connector.trino.optimization;

import org.apache.fluss.connector.trino.handle.FlussColumnHandle;
import org.apache.fluss.connector.trino.handle.FlussTableHandle;

import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Handles partition pruning optimization for Fluss connector.
 * 
 * <p>Partition pruning eliminates unnecessary partition scans by analyzing
 * predicates on partition key columns.
 */
public class FlussPartitionPruning {

    private static final Logger log = Logger.get(FlussPartitionPruning.class);

    @Inject
    public FlussPartitionPruning() {
    }

    /**
     * Apply partition pruning to the table handle.
     */
    public FlussTableHandle applyPartitionPruning(
            FlussTableHandle tableHandle,
            TupleDomain<ColumnHandle> constraint) {
        
        if (constraint.isAll()) {
            log.debug("No partition filters to apply for table: %s", tableHandle.getTableName());
            return tableHandle;
        }

        // Extract partition key predicates
        List<String> partitionFilters = extractPartitionFilters(constraint);
        
        if (partitionFilters.isEmpty()) {
            log.debug("No partition key predicates found for table: %s", tableHandle.getTableName());
            return tableHandle;
        }

        log.debug("Applying partition pruning for table: %s with %d filters",
                tableHandle.getTableName(), partitionFilters.size());

        return tableHandle.withPartitionFilters(partitionFilters);
    }

    /**
     * Extract partition filters from constraint.
     */
    private List<String> extractPartitionFilters(TupleDomain<ColumnHandle> constraint) {
        List<String> filters = new ArrayList<>();
        
        if (constraint.getDomains().isEmpty()) {
            return filters;
        }

        Map<ColumnHandle, Domain> domains = constraint.getDomains().get();
        
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            ColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            
            if (columnHandle instanceof FlussColumnHandle) {
                FlussColumnHandle flussColumn = (FlussColumnHandle) columnHandle;
                
                // Only process partition key columns
                if (flussColumn.isPartitionKey()) {
                    String filter = convertDomainToFilter(flussColumn, domain);
                    if (filter != null) {
                        filters.add(filter);
                    }
                }
            }
        }
        
        return filters;
    }

    /**
     * Convert a domain to a filter expression.
     */
    private String convertDomainToFilter(FlussColumnHandle column, Domain domain) {
        if (domain.isNullAllowed() && domain.isAll()) {
            return null;
        }
        
        StringBuilder filter = new StringBuilder();
        filter.append(column.getName());
        
        if (domain.getValues().isNone()) {
            return column.getName() + " IS NULL";
        }
        
        if (domain.getValues().isAll()) {
            return column.getName() + " IS NOT NULL";
        }
        
        io.trino.spi.predicate.ValueSet values = domain.getValues();
        
        if (values.isDiscreteSet()) {
            // Discrete values - create IN filter
            List<Object> discreteValues = new ArrayList<>();
            values.getDiscreteSet().forEach(discreteValues::add);
            
            if (discreteValues.size() == 1) {
                filter.append(" = '").append(discreteValues.get(0)).append("'");
            } else {
                filter.append(" IN (");
                for (int i = 0; i < discreteValues.size(); i++) {
                    if (i > 0) filter.append(", ");
                    filter.append("'").append(discreteValues.get(i)).append("'");
                }
                filter.append(")");
            }
        } else {
            // Range values - create range filter
            io.trino.spi.predicate.Ranges ranges = (io.trino.spi.predicate.Ranges) values;
            List<String> rangeFilters = new ArrayList<>();
            
            for (io.trino.spi.predicate.Range range : ranges.getOrderedRanges()) {
                if (range.isSingleValue()) {
                    rangeFilters.add(column.getName() + " = '" + range.getSingleValue() + "'");
                } else {
                    List<String> conditions = new ArrayList<>();
                    if (!range.isLowUnbounded()) {
                        String op = range.isLowInclusive() ? ">=" : ">";
                        conditions.add(column.getName() + " " + op + " '" + range.getLowBoundedValue() + "'");
                    }
                    if (!range.isHighUnbounded()) {
                        String op = range.isHighInclusive() ? "<=" : "<";
                        conditions.add(column.getName() + " " + op + " '" + range.getHighBoundedValue() + "'");
                    }
                    if (!conditions.isEmpty()) {
                        rangeFilters.add("(" + String.join(" AND ", conditions) + ")");
                    }
                }
            }
            
            if (rangeFilters.isEmpty()) {
                return null;
            } else if (rangeFilters.size() == 1) {
                return rangeFilters.get(0);
            } else {
                return "(" + String.join(" OR ", rangeFilters) + ")";
            }
        }
        
        return filter.toString();
    }

    /**
     * Estimate partition reduction from pruning.
     */
    public double estimatePartitionReduction(int totalPartitions, int prunedPartitions) {
        if (totalPartitions == 0 || prunedPartitions >= totalPartitions) {
            return 0.0;
        }
        return 1.0 - ((double) prunedPartitions / totalPartitions);
    }
}
