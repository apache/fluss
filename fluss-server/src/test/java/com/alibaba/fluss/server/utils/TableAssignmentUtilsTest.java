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

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.exception.InvalidBucketsException;
import com.alibaba.fluss.exception.InvalidReplicationFactorException;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link com.alibaba.fluss.server.utils.TableAssignmentUtils} . */
class TableAssignmentUtilsTest {

    @Test
    void testTableAssignment() {
        // should throw exception since servers is empty
        assertThatThrownBy(() -> TableAssignmentUtils.generateAssignment(5, -1, new int[0]))
                .isInstanceOf(InvalidReplicationFactorException.class);

        // should throw exception since the buckets is less than 0
        assertThatThrownBy(() -> TableAssignmentUtils.generateAssignment(-1, 3, new int[] {0, 1}))
                .isInstanceOf(InvalidBucketsException.class);

        // should throw exception since the server is less than replication factor
        assertThatThrownBy(() -> TableAssignmentUtils.generateAssignment(5, 3, new int[] {0, 1}))
                .isInstanceOf(InvalidReplicationFactorException.class);

        // should throw exception since replication factor is less than 0
        assertThatThrownBy(() -> TableAssignmentUtils.generateAssignment(5, -1, new int[] {0, 1}))
                .isInstanceOf(InvalidReplicationFactorException.class);

        // test replica factor 1
        TableAssignment tableAssignment =
                TableAssignmentUtils.generateAssignment(3, 1, new int[] {0, 1, 2, 3}, 0, 0);
        TableAssignment expectedAssignment =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(0))
                        .add(1, BucketAssignment.of(1))
                        .add(2, BucketAssignment.of(2))
                        .build();
        assertThat(tableAssignment).isEqualTo(expectedAssignment);

        // test replica factor 3
        tableAssignment =
                TableAssignmentUtils.generateAssignment(3, 3, new int[] {0, 1, 2, 3}, 1, 0);
        expectedAssignment =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(1, 2, 3))
                        .add(1, BucketAssignment.of(2, 3, 0))
                        .add(2, BucketAssignment.of(3, 0, 1))
                        .build();
        assertThat(tableAssignment).isEqualTo(expectedAssignment);

        // test with 10 buckets and 3 replies
        tableAssignment =
                TableAssignmentUtils.generateAssignment(10, 3, new int[] {0, 1, 2, 3, 4}, 0, 0);
        expectedAssignment =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(0, 1, 2))
                        .add(1, BucketAssignment.of(1, 2, 3))
                        .add(2, BucketAssignment.of(2, 3, 4))
                        .add(3, BucketAssignment.of(3, 4, 0))
                        .add(4, BucketAssignment.of(4, 0, 1))
                        .add(5, BucketAssignment.of(0, 2, 3))
                        .add(6, BucketAssignment.of(1, 3, 4))
                        .add(7, BucketAssignment.of(2, 4, 0))
                        .add(8, BucketAssignment.of(3, 0, 1))
                        .add(9, BucketAssignment.of(4, 1, 2))
                        .build();
        assertThat(tableAssignment).isEqualTo(expectedAssignment);
    }
}
