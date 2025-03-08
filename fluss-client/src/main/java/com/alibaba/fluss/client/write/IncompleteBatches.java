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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

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
