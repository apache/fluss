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

package org.apache.fluss.client.metadata;

import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * An abstraction for fetching metadata and rebuilding cluster. This interface allows dependency
 * injection for testing purposes.
 */
@FunctionalInterface
public interface ClusterMetadataFetcher {

    /**
     * Fetch metadata and rebuild cluster asynchronously.
     *
     * @param gateway the gateway to send request
     * @param partialUpdate whether to perform partial update (merge with existing cluster)
     * @param originCluster the original cluster to merge with (if partial update)
     * @param tablePaths tables to request metadata for
     * @param tablePartitions partitions to request metadata for
     * @param tablePartitionIds partition ids to request metadata for
     * @return a future that completes with the new cluster
     */
    CompletableFuture<Cluster> fetch(
            AdminReadOnlyGateway gateway,
            boolean partialUpdate,
            Cluster originCluster,
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitions,
            @Nullable Collection<Long> tablePartitionIds);
}
