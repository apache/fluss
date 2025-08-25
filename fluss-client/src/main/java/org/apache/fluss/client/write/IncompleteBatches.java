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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A thread-safe helper class to hold batches that haven't been acknowledged yet (including those
 * which have and have not been sent).
 */
@ThreadSafe
@Internal
final class IncompleteBatches {
    private final Set<WriteBatch> incomplete;

    public IncompleteBatches() {
        this.incomplete = new HashSet<>();
    }

    public void add(WriteBatch batch) {
        synchronized (incomplete) {
            this.incomplete.add(batch);
        }
    }

    public void remove(WriteBatch batch) {
        synchronized (incomplete) {
            boolean removed = this.incomplete.remove(batch);
            if (!removed) {
                throw new IllegalStateException(
                        "Remove from the incomplete set failed. This should be impossible.");
            }
        }
    }

    public Iterable<WriteBatch> copyAll() {
        synchronized (incomplete) {
            return new ArrayList<>(this.incomplete);
        }
    }

    public Iterable<WriteBatch.RequestFuture> requestResults() {
        synchronized (incomplete) {
            return incomplete.stream()
                    .map(WriteBatch::getRequestFuture)
                    .collect(Collectors.toList());
        }
    }

    public boolean isEmpty() {
        synchronized (incomplete) {
            return incomplete.isEmpty();
        }
    }
}
