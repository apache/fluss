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

package org.apache.fluss.flink.sink.shuffle;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Assigns subtasks to partition keys based on bucketing and weighted distribution.
 *
 * <p>Unlike the parent class {@link WeightedRandomAssignment } which randomly assigns subtasks
 * based on weights, this implementation ensures that the subtasks assigned to a partition key are
 * evenly distributed across all bucket IDs, with each bucket ID mapped to one or several subtasks
 * according to the configured weights.
 */
@Internal
public class WeightedBucketIdAssignment extends WeightedRandomAssignment {
    private final double bucketWeights;
    private final int bucketNum;
    private final KeyEncoder bucketKeyEncoder;
    private final BucketingFunction bucketingFunction;

    @VisibleForTesting
    WeightedBucketIdAssignment(
            List<Integer> assignedSubtasks,
            List<Long> subtaskWeights,
            int bucketNum,
            KeyEncoder bucketKeyEncoder,
            BucketingFunction bucketingFunction,
            Random random) {
        super(assignedSubtasks, subtaskWeights, random);
        this.bucketNum = bucketNum;
        this.bucketWeights = keyWeight * 1.0 / bucketNum;
        this.bucketKeyEncoder = bucketKeyEncoder;
        this.bucketingFunction = bucketingFunction;
    }

    @Override
    public int select(InternalRow row) {
        byte[] bucketKeyByte = bucketKeyEncoder.encodeKey(row);
        int bucketId = bucketingFunction.bucketing(bucketKeyByte, bucketNum);
        checkArgument(bucketId >= 0 && bucketId < bucketNum, "Invalid bucketId: %s", bucketId);
        double calculatedWeight =
                Math.floor(nextDouble(bucketId * bucketWeights, (bucketId + 1) * bucketWeights));
        int index = Arrays.binarySearch(cumulativeWeights, calculatedWeight);
        int position = Math.abs(index + 1);
        if (position >= assignedSubtasks.size()) {
            throw new IllegalStateException(
                    String.format(
                            "Invalid selected position: out of range. key weight = %s, random number = %s, cumulative weights array = %s",
                            keyWeight, calculatedWeight, Arrays.toString(cumulativeWeights)));
        }
        return assignedSubtasks.get(position);
    }
}
