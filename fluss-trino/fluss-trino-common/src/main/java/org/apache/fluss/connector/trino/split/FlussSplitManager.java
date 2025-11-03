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
        
        // Create splits based on table buckets
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        
        // Get bucket count from table descriptor
        TableDescriptor tableDescriptor = tableInfo.getTableDescriptor();
        Optional<Integer> bucketCount = tableDescriptor.getDistribution().getBucketCount();
        
        if (bucketCount.isPresent()) {
            int numBuckets = bucketCount.get();
            log.debug("Table %s has %d buckets", tablePath, numBuckets);
            
            // Create a split for each bucket
            for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), bucketId);
                FlussSplit split = new FlussSplit(tablePath, tableBucket);
                splits.add(split);
            }
        } else {
            // If no bucket distribution defined, create a single split
            log.debug("Table %s has no explicit bucket distribution, creating single split", tablePath);
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
            FlussSplit split = new FlussSplit(tablePath, tableBucket);
            splits.add(split);
        }
        
        List<ConnectorSplit> splitList = splits.build();
        log.debug("Generated %d splits for table: %s", splitList.size(), tablePath);
        
        return new FixedSplitSource(splitList);
    }
}
