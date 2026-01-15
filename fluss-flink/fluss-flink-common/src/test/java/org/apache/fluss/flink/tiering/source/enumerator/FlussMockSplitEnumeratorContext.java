/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.tiering.source.enumerator;

import org.apache.fluss.utils.MapUtils;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A mock extension of {@link MockSplitEnumeratorContext} for testing purposes, support registering
 * source readers with attempt number.
 *
 * @param <SplitT> The type of {@link SourceSplit} used by the source.
 */
class FlussMockSplitEnumeratorContext<SplitT extends SourceSplit>
        extends MockSplitEnumeratorContext<SplitT> {

    private final ConcurrentMap<Integer, ConcurrentMap<Integer, ReaderInfo>> registeredReaders;

    public FlussMockSplitEnumeratorContext(int parallelism) {
        super(parallelism);
        this.registeredReaders = MapUtils.newConcurrentHashMap();
    }

    public void registerSourceReader(int subtaskId, int attemptNumber, String location) {
        final Map<Integer, ReaderInfo> attemptReaders =
                registeredReaders.computeIfAbsent(subtaskId, k -> MapUtils.newConcurrentHashMap());
        checkState(
                !attemptReaders.containsKey(attemptNumber),
                "ReaderInfo of subtask %s (#%s) already exists.",
                subtaskId,
                attemptNumber);
        attemptReaders.put(attemptNumber, new ReaderInfo(subtaskId, location));
    }

    @Override
    public void registerReader(ReaderInfo readerInfo) {
        this.registerSourceReader(readerInfo.getSubtaskId(), 0, readerInfo.getLocation());
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
        final Map<Integer, ReaderInfo> readers = new HashMap<>();
        for (Map.Entry<Integer, ConcurrentMap<Integer, ReaderInfo>> entry :
                registeredReaders.entrySet()) {
            final int subtaskIndex = entry.getKey();
            final Map<Integer, ReaderInfo> attemptReaders = entry.getValue();
            if (!attemptReaders.isEmpty()) {
                int earliestAttempt = Collections.min(attemptReaders.keySet());
                readers.put(subtaskIndex, attemptReaders.get(earliestAttempt));
            }
        }
        return Collections.unmodifiableMap(readers);
    }

    @Override
    public Map<Integer, Map<Integer, ReaderInfo>> registeredReadersOfAttempts() {
        return Collections.unmodifiableMap(registeredReaders);
    }
}
