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

import org.apache.fluss.connector.trino.config.FlussConnectorConfig;
import org.apache.fluss.connector.trino.handle.FlussColumnHandle;
import org.apache.fluss.connector.trino.handle.FlussTableHandle;

import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Handles predicate pushdown optimization for Fluss connector.
 * 
 * <p>This class converts Trino predicates into Fluss-compatible filters
 * that can be pushed down to the storage layer for efficient filtering.
 */
public class FlussPredicatePushdown {

    private static final Logger log = Logger.get(FlussPredicatePushdown.class);

    private final FlussConnectorConfig config;

    @Inject
    public FlussPredicatePushdown(FlussConnectorConfig config) {
        this.config = requireNonNull(config, "config is null");
    }

    /**
     * Apply predicate pushdown to the table handle.
     */
    public FlussTableHandle applyPredicatePushdown(
            FlussTableHandle tableHandle,
            TupleDomain<ColumnHandle> constraint) {
        
        if (!config.isPredicatePushdownEnabled()) {
            log.debug("Predicate pushdown is disabled");
            return tableHandle;
        }

        if (constraint.isAll()) {
            log.debug("No predicates to push down for table: %s", tableHandle.getTableName());
            return tableHandle;
        }

        if (constraint.isNone()) {
            log.debug("Constraint is NONE (always false) for table: %s", tableHandle.getTableName());
            return tableHandle.withConstraint(constraint);
        }

        log.debug("Applying predicate pushdown for table: %s with constraint: %s",
                tableHandle.getTableName(), constraint);

        // Apply the constraint to the table handle
        return tableHandle.withConstraint(constraint);
    }

    /**
     * Check if a predicate can be pushed down for the given column.
     */
    public boolean canPushdownPredicate(FlussColumnHandle columnHandle) {
        // Primary key columns are best for predicate pushdown
        if (columnHandle.isPrimaryKey()) {
            return true;
        }
        
        // Partition key columns support partition pruning
        if (columnHandle.isPartitionKey()) {
            return true;
        }
        
        // Other columns can also benefit from predicate pushdown
        return true;
    }

    /**
     * Extract pushdown-able predicates from the constraint.
     */
    public Optional<TupleDomain<ColumnHandle>> extractPushdownPredicates(
            TupleDomain<ColumnHandle> constraint,
            Map<String, ColumnHandle> columnHandles) {
        
        if (constraint.isAll() || constraint.isNone()) {
            return Optional.of(constraint);
        }

        if (!constraint.getDomains().isPresent()) {
            return Optional.of(constraint);
        }

        Map<ColumnHandle, Domain> domains = constraint.getDomains().get();
        
        // Analyze each domain to determine if it can be pushed down
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            ColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            
            if (columnHandle instanceof FlussColumnHandle) {
                FlussColumnHandle flussColumn = (FlussColumnHandle) columnHandle;
                
                // Check if this predicate can be pushed down
                if (canPushdownPredicate(flussColumn)) {
                    String filter = convertDomainToFilter(flussColumn, domain);
                    log.debug("Converted predicate for column %s: %s", 
                            flussColumn.getName(), filter);
                }
            }
        }

        return Optional.of(constraint);
    }

    /**
     * Convert a Trino Domain to a Fluss-compatible filter expression.
     */
    private String convertDomainToFilter(FlussColumnHandle column, Domain domain) {
        StringBuilder filter = new StringBuilder();
        filter.append(column.getName());
        
        if (domain.isNullAllowed() && domain.getValues().isNone()) {
            filter.append(" IS NULL");
            return filter.toString();
        }
        
        if (domain.getValues().isAll()) {
            if (domain.isNullAllowed()) {
                return column.getName() + " IS NOT NULL OR " + column.getName() + " IS NULL";
            }
            return column.getName() + " IS NOT NULL";
        }
        
        ValueSet values = domain.getValues();
        if (values.isDiscreteSet()) {
            // Handle IN predicate
            List<Object> discreteValues = new ArrayList<>();
            values.getDiscreteSet().forEach(discreteValues::add);
            if (discreteValues.size() == 1) {
                filter.append(" = ").append(formatValue(discreteValues.get(0)));
            } else {
                filter.append(" IN (");
                for (int i = 0; i < discreteValues.size(); i++) {
                    if (i > 0) filter.append(", ");
                    filter.append(formatValue(discreteValues.get(i)));
                }
                filter.append(")");
            }
        } else {
            // Handle range predicates
            List<String> rangeFilters = new ArrayList<>();
            for (Range range : values.getRanges().getOrderedRanges()) {
                rangeFilters.add(convertRangeToFilter(column.getName(), range));
            }
            if (rangeFilters.size() == 1) {
                return rangeFilters.get(0);
            } else {
                filter = new StringBuilder("(");
                for (int i = 0; i < rangeFilters.size(); i++) {
                    if (i > 0) filter.append(" OR ");
                    filter.append(rangeFilters.get(i));
                }
                filter.append(")");
            }
        }
        
        if (domain.isNullAllowed()) {
            filter.insert(0, "(").append(" OR ").append(column.getName()).append(" IS NULL)");
        }
        
        return filter.toString();
    }
    
    private String convertRangeToFilter(String columnName, Range range) {
        if (range.isSingleValue()) {
            return columnName + " = " + formatValue(range.getSingleValue());
        }
        
        List<String> conditions = new ArrayList<>();
        
        if (!range.isLowUnbounded()) {
            String operator = range.isLowInclusive() ? ">=" : ">";
            conditions.add(columnName + " " + operator + " " + formatValue(range.getLowBoundedValue()));
        }
        
        if (!range.isHighUnbounded()) {
            String operator = range.isHighInclusive() ? "<=" : "<";
            conditions.add(columnName + " " + operator + " " + formatValue(range.getHighBoundedValue()));
        }
        
        if (conditions.isEmpty()) {
            return columnName + " IS NOT NULL";
        } else if (conditions.size() == 1) {
            return conditions.get(0);
        } else {
            return "(" + String.join(" AND ", conditions) + ")";
        }
    }
    
    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + value.toString().replace("'", "''") + "'";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Boolean) {
            return value.toString().toUpperCase();
        }
        // For other types, use string representation
        return "'" + value.toString().replace("'", "''") + "'";
    }
}
