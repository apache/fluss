/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.zk.data;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The assignment of a bucket. The assignment is represented as a list of integers where each
 * integer is the replica ID (tabletserver id).
 *
 * @see TableAssignment is consist of {@link BucketAssignment} for each bucket.
 */
public class BucketAssignment {
    private final List<Integer> replicas;

    public BucketAssignment(List<Integer> replicas) {
        this.replicas = replicas;
    }

    public static BucketAssignment of(Integer... replicas) {
        return new BucketAssignment(Arrays.asList(replicas));
    }

    public List<Integer> getReplicas() {
        return replicas;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BucketAssignment that = (BucketAssignment) o;
        return Objects.equals(replicas, that.replicas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicas);
    }

    @Override
    public String toString() {
        return replicas.toString();
    }
}
