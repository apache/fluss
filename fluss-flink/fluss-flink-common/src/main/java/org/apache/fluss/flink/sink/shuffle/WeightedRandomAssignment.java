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
import org.apache.fluss.row.InternalRow;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Partition assignment strategy that randomly distributes records to subtasks based on configured
 * weights.
 *
 * <p>This assignment strategy enables weighted random distribution of records to subtasks, allowing
 * for more balanced load distribution across downstream subtasks. The assignment uses a weighted
 * random algorithm where subtasks with higher weights have a proportionally higher probability of
 * being selected.
 *
 * <p>NOTE: This class is inspired from Iceberg project.
 */
@Internal
public class WeightedRandomAssignment implements PartitionAssignment {
    protected final List<Integer> assignedSubtasks;
    protected final List<Long> subtaskWeights;
    protected final long keyWeight;
    protected final double[] cumulativeWeights;
    private final Random random;

    /**
     * @param assignedSubtasks assigned subtasks for this key. It could be a single subtask. It
     *     could also be multiple subtasks if the key has heavy weight that should be handled by
     *     multiple subtasks.
     * @param subtaskWeights assigned weight for each subtask. E.g., if the keyWeight is 27 and the
     *     key is assigned to 3 subtasks, subtaskWeights could contain values as [10, 10, 7] for
     *     target weight of 10 per subtask.
     */
    WeightedRandomAssignment(
            List<Integer> assignedSubtasks, List<Long> subtaskWeights, Random random) {
        checkArgument(
                assignedSubtasks != null && !assignedSubtasks.isEmpty(),
                "Invalid assigned subtasks: null or empty");
        checkArgument(
                subtaskWeights != null && !subtaskWeights.isEmpty(),
                "Invalid assigned subtasks weights: null or empty");
        checkArgument(
                assignedSubtasks.size() == subtaskWeights.size(),
                "Invalid assignment: size mismatch (tasks length = %s, weights length = %s)",
                assignedSubtasks.size(),
                subtaskWeights.size());

        this.assignedSubtasks = assignedSubtasks;
        this.subtaskWeights = subtaskWeights;
        this.keyWeight = subtaskWeights.stream().mapToLong(Long::longValue).sum();
        this.cumulativeWeights = new double[subtaskWeights.size()];
        long cumulativeWeight = 0;
        for (int i = 0; i < subtaskWeights.size(); ++i) {
            cumulativeWeight += subtaskWeights.get(i);
            cumulativeWeights[i] = cumulativeWeight;
        }
        this.random = random;
    }

    /**
     * Select a subtask for the key. If bucket key is existed , same key will be assigned to the
     * same subtask.
     *
     * @return subtask id
     */
    @Override
    public int select(InternalRow row) {
        if (assignedSubtasks.size() == 1) {
            // only choice. no need to run random number generator.
            return assignedSubtasks.get(0);
        } else {
            double randomNumber = nextDouble(0, keyWeight);
            int index = Arrays.binarySearch(cumulativeWeights, randomNumber);
            // choose the subtask where randomNumber < cumulativeWeights[pos].
            // this works regardless whether index is negative or not.
            int position = Math.abs(index + 1);
            if (position >= assignedSubtasks.size()) {
                throw new IllegalStateException(
                        String.format(
                                "Invalid selected position: out of range. key weight = %s, random number = %s, cumulative weights array = %s",
                                keyWeight, randomNumber, Arrays.toString(cumulativeWeights)));
            }
            return assignedSubtasks.get(position);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WeightedRandomAssignment that = (WeightedRandomAssignment) o;
        return keyWeight == that.keyWeight
                && Objects.equals(assignedSubtasks, that.assignedSubtasks)
                && Objects.equals(subtaskWeights, that.subtaskWeights)
                && Arrays.equals(cumulativeWeights, that.cumulativeWeights);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                assignedSubtasks, subtaskWeights, keyWeight, Arrays.hashCode(cumulativeWeights));
    }

    @Override
    public String toString() {
        return "KeyAssignment{"
                + "assignedSubtasks="
                + assignedSubtasks
                + ", subtaskWeights="
                + subtaskWeights
                + ", keyWeight="
                + keyWeight
                + ", cumulativeWeights="
                + Arrays.toString(cumulativeWeights)
                + '}';
    }

    /**
     * This implementation is based on {@link
     * java.util.concurrent.ThreadLocalRandom#nextDouble(double, double)}, but uses a {@link Random}
     * instance instead of ThreadLocalRandom. This allows injecting a MockRandom with a fixed seed
     * in tests, which improves test stability and reproducibility by ensuring deterministic random
     * behavior.
     */
    protected double nextDouble(double origin, double bound) {
        double r = random.nextDouble();
        r = r * (bound - origin) + origin;
        if (r >= bound) {
            r = Double.longBitsToDouble(Double.doubleToLongBits(bound) - 1);
        }
        return r;
    }
}
