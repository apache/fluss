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

import java.util.LinkedHashMap;
import java.util.Map;

/** Leader-local logical progress for a first-row writer with at most five in-flight requests. */
final class FirstRowWriterState {
    private static final int MAX_INFLIGHT_BATCHES = 5;
    private final Map<Integer, Long> barriers = new LinkedHashMap<>();
    private int lastSequence;
    private long lastAccessMs;

    FirstRowWriterState(int acknowledgedSequence, long now) {
        lastSequence = acknowledgedSequence;
        lastAccessMs = now;
    }

    long lastAccessMs() {
        return lastAccessMs;
    }

    /** Returns an existing commit barrier, or -1 when the request must be processed. */
    long prepare(int sequence, int acknowledgedSequence, long committedOffset, long now) {
        if (sequence < 0 || acknowledgedSequence < -1) {
            throw new OutOfOrderSequenceException("Invalid first-row writer sequence");
        }
        lastAccessMs = now;
        if (acknowledgedSequence >= 0 && ahead(acknowledgedSequence, lastSequence)) {
            lastSequence = acknowledgedSequence;
        }
        Long barrier = barriers.get(sequence);
        if (barrier != null) {
            return barrier;
        }
        if (sequence == next(lastSequence)) {
            return -1;
        }
        if (sequence == acknowledgedSequence
                || (acknowledgedSequence >= 0 && !ahead(sequence, acknowledgedSequence))) {
            return committedOffset;
        }
        throw new OutOfOrderSequenceException(
                "Out of order first-row batch sequence: "
                        + sequence
                        + " (incoming), "
                        + lastSequence
                        + " (last processed)");
    }

    void complete(int sequence, long barrier) {
        lastSequence = sequence;
        barriers.put(sequence, barrier);
        if (barriers.size() > MAX_INFLIGHT_BATCHES) {
            barriers.remove(barriers.keySet().iterator().next());
        }
    }

    private static int next(int sequence) {
        return sequence == Integer.MAX_VALUE ? 0 : sequence + 1;
    }

    private static boolean ahead(int sequence, int previous) {
        if (previous == -1) {
            return true;
        }
        long distance = ((long) sequence - previous) & Integer.MAX_VALUE;
        return distance > 0 && distance <= Integer.MAX_VALUE / 2;
    }
}
