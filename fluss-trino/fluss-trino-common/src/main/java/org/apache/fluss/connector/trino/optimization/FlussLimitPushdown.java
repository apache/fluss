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
import org.apache.fluss.connector.trino.handle.FlussTableHandle;

import io.airlift.log.Logger;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * Handles limit pushdown optimization for Fluss connector.
 * 
 * <p>Limit pushdown reduces data transfer by limiting the number of rows
 * read from storage.
 */
public class FlussLimitPushdown {

    private static final Logger log = Logger.get(FlussLimitPushdown.class);

    private final FlussConnectorConfig config;

    @Inject
    public FlussLimitPushdown(FlussConnectorConfig config) {
        this.config = requireNonNull(config, "config is null");
    }

    /**
     * Apply limit pushdown to the table handle.
     * 
     * <p>This method applies the limit constraint to reduce the amount of data
     * read from storage.
     */
    public FlussTableHandle applyLimitPushdown(
            FlussTableHandle tableHandle,
            long limit) {
        
        if (!config.isLimitPushdownEnabled()) {
            log.debug("Limit pushdown is disabled");
            return tableHandle;
        }

        if (limit <= 0) {
            log.debug("Invalid limit value: %d", limit);
            return tableHandle;
        }

        log.debug("Applying limit pushdown for table: %s with limit: %d",
                tableHandle.getTableName(), limit);

        // Analyze limit to determine pushdown effectiveness
        long optimizedLimit = analyzeLimitForPushdown(tableHandle, limit);
        
        return tableHandle.withLimit(optimizedLimit);
    }
    
    /**
     * Analyze limit to optimize pushdown.
     */
    private long analyzeLimitForPushdown(FlussTableHandle tableHandle, long limit) {
        // In a full implementation, we would:
        // 1. Estimate total row count
        // 2. Check if limit is beneficial
        // 3. Adjust limit based on distribution
        // 4. Consider other optimizations
        
        // For now, return limit as-is but log analysis
        log.debug("Analyzing limit %d for table: %s", limit, tableHandle.getTableName());
        
        return limit;
    }

    /**
     * Check if limit pushdown is beneficial for this query.
     */
    public boolean isBeneficial(long limit, long estimatedRowCount) {
        // Limit pushdown is beneficial when limit is much smaller than total rows
        return limit > 0 && limit < estimatedRowCount * 0.5;
    }

    /**
     * Estimate data reduction from limit pushdown.
     */
    public double estimateDataReduction(long limit, long totalRows) {
        if (totalRows == 0 || limit >= totalRows) {
            return 0.0;
        }
        return 1.0 - ((double) limit / totalRows);
    }
}
