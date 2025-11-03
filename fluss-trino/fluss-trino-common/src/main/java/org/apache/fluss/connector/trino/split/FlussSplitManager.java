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

package org.apache.fluss.connector.trino.split;

import org.apache.fluss.connector.trino.connection.FlussClientManager;
import org.apache.fluss.connector.trino.handle.FlussTableHandle;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Split manager for Fluss connector.
 * 
 * <p>This class is responsible for generating splits based on Fluss table buckets.
 * Each split represents a bucket in Fluss which is the unit of parallelism.
 */
public class FlussSplitManager implements ConnectorSplitManager {

    private static final Logger log = Logger.get(FlussSplitManager.class);

    private final FlussClientManager clientManager;

    @Inject
    public FlussSplitManager(FlussClientManager clientManager) {
        this.clientManager = requireNonNull(clientManager, "clientManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        
        FlussTableHandle flussTable = (FlussTableHandle) tableHandle;
        TableInfo tableInfo = flussTable.getTableInfo();
        TablePath tablePath = flussTable.getTablePath();
        
        log.debug("Generating splits for table: %s", tablePath);
        
        // Apply dynamic filters if available
        if (!dynamicFilter.getCurrentPredicate().isAll()) {
            log.debug("Applying dynamic filter for table: %s", tablePath);
            // In a full implementation, we would use dynamic filters for partition pruning
        }
        
        // Create splits based on table buckets
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        
        // Get bucket count from table descriptor
        TableDescriptor tableDescriptor = tableInfo.getTableDescriptor();
        Optional<Integer> bucketCount = tableDescriptor.getDistribution().getBucketCount();
        
        if (bucketCount.isPresent()) {
            int numBuckets = bucketCount.get();
            log.debug("Table %s has %d buckets", tablePath, numBuckets);
            
            // Apply performance tuning based on configuration
            int maxSplits = Math.min(numBuckets, getMaxSplitsPerRequest(session));
            
            // Create a split for each bucket
            for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), bucketId);
                List<HostAddress> addresses = getPreferredHosts(tableBucket);
                FlussSplit split = new FlussSplit(tablePath, tableBucket, addresses);
                splits.add(split);
                
                // Apply rate limiting
                if ((bucketId + 1) % maxSplits == 0 && bucketId < numBuckets - 1) {
                    // In a production implementation, we might want to batch splits
                    // for better performance control
                    log.debug("Generated %d splits so far for table: %s", bucketId + 1, tablePath);
                }
            }
        } else {
            // If no bucket distribution defined, create a single split
            log.debug("Table %s has no explicit bucket distribution, creating single split", tablePath);
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
            List<HostAddress> addresses = getPreferredHosts(tableBucket);
            FlussSplit split = new FlussSplit(tablePath, tableBucket, addresses);
            splits.add(split);
        }
        
        List<ConnectorSplit> splitList = splits.build();
        log.debug("Generated %d splits for table: %s", splitList.size(), tablePath);
        
        return new FixedSplitSource(splitList);
    }
    
    /**
     * Get the maximum number of splits per request based on session and configuration.
     */
    private int getMaxSplitsPerRequest(ConnectorSession session) {
        // In a full implementation, we could get this from session properties
        // For now, we use a default value
        return 100;
    }
    
    /**
     * Get preferred hosts for a table bucket based on data locality.
     * 
     * <p>In a production implementation, this would query Fluss metadata
     * to determine which tablet servers host the data for this bucket.
     */
    private List<HostAddress> getPreferredHosts(TableBucket tableBucket) {
        // In a full implementation:
        // 1. Query metadata to get tablet server locations for this bucket
        // 2. Return preferred hosts for data locality
        // 3. Handle server failures and load balancing
        
        // For now, return empty list to let Trino handle scheduling
        return List.of();
    }
}
