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

package org.apache.fluss.server.coordinator.remote;

import org.apache.fluss.fs.FsPath;

import java.util.Collections;
import java.util.List;

/**
 * Weighted Round-robin remote data dir selector using Interleaved (Smooth) Weighted Round-Robin
 * algorithm.
 *
 * <p>This implementation uses the smooth weighted round-robin algorithm (also known as interleaved
 * weighted round-robin), which distributes selections more evenly compared to traditional weighted
 * round-robin. Instead of selecting the same node consecutively based on its weight, it interleaves
 * selections to achieve a smoother distribution.
 *
 * <p>Algorithm:
 *
 * <ol>
 *   <li>Each node maintains a currentWeight initialized to 0
 *   <li>On each selection: add the node's configured weight to its currentWeight
 *   <li>Select the node with the highest currentWeight
 *   <li>Subtract the total weight sum from the selected node's currentWeight
 * </ol>
 *
 * <p>Example: For nodes A, B, C with weights 5, 1, 1 (total=7), the selection sequence would be: A,
 * A, B, A, C, A, A (instead of A, A, A, A, A, B, C in traditional WRR).
 */
public class WeightedRoundRobinRemoteDirSelector implements RemoteDirSelector {

    private final FsPath defaultRemoteDataDir;
    private final List<FsPath> remoteDataDirs;
    private final int[] weights;
    private final int totalWeight;

    // Current weights for each node, used in smooth weighted round-robin
    private final int[] currentWeights;

    // Lock object for thread safety
    private final Object lock = new Object();

    public WeightedRoundRobinRemoteDirSelector(
            FsPath defaultRemoteDataDir, List<FsPath> remoteDataDirs, List<Integer> weights) {
        this.defaultRemoteDataDir = defaultRemoteDataDir;
        this.remoteDataDirs = Collections.unmodifiableList(remoteDataDirs);

        // Convert weights list to array for better performance
        this.weights = new int[weights.size()];
        int sum = 0;
        for (int i = 0; i < weights.size(); i++) {
            this.weights[i] = weights.get(i);
            sum += this.weights[i];
        }
        this.totalWeight = sum;

        // Initialize current weights to 0
        this.currentWeights = new int[remoteDataDirs.size()];
    }

    @Override
    public FsPath nextDataDir() {
        if (remoteDataDirs.isEmpty() || totalWeight == 0) {
            return defaultRemoteDataDir;
        }

        synchronized (lock) {
            int selectedIndex = -1;
            int maxCurrentWeight = Integer.MIN_VALUE;

            // Step 1 & 2: Add weight to currentWeight and find the max
            for (int i = 0; i < remoteDataDirs.size(); i++) {
                currentWeights[i] += weights[i];
                if (currentWeights[i] > maxCurrentWeight) {
                    maxCurrentWeight = currentWeights[i];
                    selectedIndex = i;
                }
            }

            // Step 3: Subtract total weight from selected node's current weight
            currentWeights[selectedIndex] -= totalWeight;

            return remoteDataDirs.get(selectedIndex);
        }
    }
}
