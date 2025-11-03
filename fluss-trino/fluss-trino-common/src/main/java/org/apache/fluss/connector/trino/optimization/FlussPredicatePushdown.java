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
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;

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

        // In a full implementation, we would analyze the constraint domains
        // and determine which parts can be pushed down to Fluss
        return Optional.of(constraint);
    }
}
