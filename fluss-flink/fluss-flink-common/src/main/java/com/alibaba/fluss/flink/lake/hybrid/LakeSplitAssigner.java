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

package com.alibaba.fluss.flink.lake.hybrid;

import com.alibaba.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;

import static com.alibaba.fluss.flink.source.enumerator.TableBucketAssigner.assignTableBucket;

/** The assigner for lake bucket. */
public class LakeSplitAssigner implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, Long> partitionIdByName;

    public LakeSplitAssigner(Map<String, Long> partitionIdByName) {
        this.partitionIdByName = partitionIdByName;
    }

    @Nullable
    public Integer assignSplit(@Nullable String partition, int bucket, int subTasks) {
        if (partition == null) {
            return assignTableBucket(
                    // don't care about table id
                    new TableBucket(-1L, bucket), subTasks);
        } else {
            Long partitionId = partitionIdByName.get(partition);
            if (partitionId == null) {
                // we can't get the partition id by name, which means
                // the partition has been removed in Fluss,
                // we don't need to read this partition, return null directly
                return null;
            } else {
                return assignTableBucket(new TableBucket(-1, partitionId, bucket), subTasks);
            }
        }
    }
}
