/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.server.kv.autoinc;

import org.apache.fluss.server.SequenceIDCounter;

import java.util.concurrent.atomic.AtomicLong;

/** A testing implementation of {@link SequenceIDCounter} based Java {@link AtomicLong}. */
public class TestingSequenceIDCounter implements SequenceIDCounter {
    private final AtomicLong idGenerator;
    private int fetchTime;
    private final int failedTrigger;

    public TestingSequenceIDCounter(AtomicLong idGenerator) {
        this(idGenerator, Integer.MAX_VALUE);
    }

    public TestingSequenceIDCounter(AtomicLong idGenerator, int failedTrigger) {
        this.idGenerator = idGenerator;
        fetchTime = 0;
        this.failedTrigger = failedTrigger;
    }

    @Override
    public long getCurrent() {
        return idGenerator.get();
    }

    @Override
    public long getAndIncrement() {
        return idGenerator.getAndIncrement();
    }

    @Override
    public long getAndAdd(Long delta) {
        if (++fetchTime < failedTrigger) {
            return idGenerator.getAndAdd(delta);
        }
        throw new RuntimeException("Failed to get snapshot ID");
    }
}
