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

package org.apache.fluss.lake.source;

import org.apache.fluss.metadata.PartitionInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** A testing implementation of {@link Planner}. */
public class TestingPlanner implements Planner<LakeSplit> {

    private final int bucketNum;
    private final List<PartitionInfo> partitionInfos;

    public TestingPlanner(int bucketNum, List<PartitionInfo> partitionInfos) {
        this.bucketNum = bucketNum;
        this.partitionInfos = partitionInfos;
    }

    @Override
    public List<LakeSplit> plan() throws IOException {
        List<LakeSplit> splits = new ArrayList<>();

        for (PartitionInfo partitionInfo : partitionInfos) {
            for (int i = 0; i < bucketNum; i++) {
                splits.add(
                        new TestingLakeSplit(
                                i, partitionInfo.getResolvedPartitionSpec().getPartitionValues()));
            }
        }

        return splits;
    }
}
