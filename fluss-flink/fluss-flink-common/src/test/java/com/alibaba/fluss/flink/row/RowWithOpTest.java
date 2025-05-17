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

package com.alibaba.fluss.flink.row;

import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RowWithOp}. */
public class RowWithOpTest {

    @Test
    void testConstructor_withValidInputs() {
        InternalRow row = new GenericRow(2);
        RowWithOp rowWithOp = new RowWithOp(row, OperationType.APPEND);

        assertThat(rowWithOp.getRow()).isSameAs(row);
        assertThat(rowWithOp.getOperationType()).isEqualTo(OperationType.APPEND);
    }

    @Test
    void testConstructor_withNullRow_shouldThrowException() {
        assertThatThrownBy(() -> new RowWithOp(null, OperationType.APPEND))
                .isInstanceOf(NullPointerException.class);

        InternalRow row = new GenericRow(1);
        assertThatThrownBy(() -> new RowWithOp(row, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testEquals_withSameInstance() {
        InternalRow row = new GenericRow(1);
        RowWithOp rowWithOp = new RowWithOp(row, OperationType.APPEND);

        assertThat(rowWithOp).isEqualTo(rowWithOp);
    }

    @Test
    void testEquals_withEqualObjects() {
        GenericRow row1 = new GenericRow(1);
        GenericRow row2 = new GenericRow(1);

        RowWithOp rowWithOp1 = new RowWithOp(row1, OperationType.APPEND);
        RowWithOp rowWithOp2 = new RowWithOp(row2, OperationType.APPEND);

        assertThat(rowWithOp1).isEqualTo(rowWithOp2);
        assertThat(rowWithOp2).isEqualTo(rowWithOp1);
    }

    @Test
    void testEquals_withDifferentRows() {
        GenericRow row1 = new GenericRow(1);
        GenericRow row2 = new GenericRow(2); // Different field count to ensure they're different

        RowWithOp rowWithOp1 = new RowWithOp(row1, OperationType.APPEND);
        RowWithOp rowWithOp2 = new RowWithOp(row2, OperationType.APPEND);

        assertThat(rowWithOp1).isNotEqualTo(rowWithOp2);
    }

    @Test
    void testEquals_withDifferentOpTypes() {
        InternalRow row = new GenericRow(1);

        RowWithOp rowWithOp1 = new RowWithOp(row, OperationType.APPEND);
        RowWithOp rowWithOp2 = new RowWithOp(row, OperationType.DELETE);

        assertThat(rowWithOp1).isNotEqualTo(rowWithOp2);
    }

    @Test
    void testEquals_withNull() {
        InternalRow row = new GenericRow(1);
        RowWithOp rowWithOp = new RowWithOp(row, OperationType.APPEND);

        assertThat(rowWithOp).isNotEqualTo(null);
    }

    @Test
    void testHashCode_withEqualObjects() {
        GenericRow row1 = new GenericRow(1);
        GenericRow row2 = new GenericRow(1);

        RowWithOp rowWithOp1 = new RowWithOp(row1, OperationType.APPEND);
        RowWithOp rowWithOp2 = new RowWithOp(row2, OperationType.APPEND);

        assertThat(rowWithOp1.hashCode()).isEqualTo(rowWithOp2.hashCode());
    }
}
