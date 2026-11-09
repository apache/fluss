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

package org.apache.fluss.server.kv;

import org.apache.fluss.exception.OutOfOrderSequenceException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FirstRowWriterStateTest {
    @Test
    void testRetriesKeepOriginalBarrier() {
        FirstRowWriterState state = new FirstRowWriterState(-1, 0);
        assertThat(state.prepare(0, -1, 0, 1)).isEqualTo(-1);
        state.complete(0, 3);
        assertThat(state.prepare(0, -1, 100, 2)).isEqualTo(3);
        assertThatThrownBy(() -> state.prepare(2, -1, 0, 3))
                .isInstanceOf(OutOfOrderSequenceException.class);
    }

    @Test
    void testAcknowledgedPrefixRebuildsProgress() {
        FirstRowWriterState state = new FirstRowWriterState(7, 0);
        assertThat(state.prepare(8, 7, 10, 1)).isEqualTo(-1);
        state.complete(8, 10);
        assertThat(state.prepare(10, 9, 10, 2)).isEqualTo(-1);
    }

    @Test
    void testUnacknowledgedRequestIsReprocessedAfterFailover() {
        FirstRowWriterState state = new FirstRowWriterState(0, 0);
        assertThatThrownBy(() -> state.prepare(2, 0, 10, 1))
                .isInstanceOf(OutOfOrderSequenceException.class);
        assertThat(state.prepare(1, 0, 10, 2)).isEqualTo(-1);
        state.complete(1, 10);
        assertThat(state.prepare(2, 1, 10, 3)).isEqualTo(-1);
    }

    @Test
    void testSequenceBoundaryAndLateRetry() {
        FirstRowWriterState state = new FirstRowWriterState(Integer.MAX_VALUE - 1, 0);
        assertThat(state.prepare(Integer.MAX_VALUE, Integer.MAX_VALUE - 1, 4, 1)).isEqualTo(-1);
        state.complete(Integer.MAX_VALUE, 5);
        assertThat(state.prepare(0, Integer.MAX_VALUE, 5, 2)).isEqualTo(-1);
        state.complete(0, 6);
        assertThat(state.prepare(Integer.MAX_VALUE, Integer.MAX_VALUE - 1, 10, 3)).isEqualTo(5);
        assertThat(state.prepare(1, 0, 6, 4)).isEqualTo(-1);
    }

    @Test
    void testEvictedAcknowledgedRequestUsesCommittedPrefix() {
        FirstRowWriterState state = new FirstRowWriterState(-1, 0);
        for (int sequence = 0; sequence < 8; sequence++) {
            assertThat(state.prepare(sequence, sequence - 1, 0, sequence)).isEqualTo(-1);
            state.complete(sequence, sequence + 1);
        }
        assertThat(state.prepare(0, 7, 100, 10)).isEqualTo(100);
    }
}
