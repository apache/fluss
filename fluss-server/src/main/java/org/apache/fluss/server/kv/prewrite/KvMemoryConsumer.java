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

package org.apache.fluss.server.kv.prewrite;

import org.apache.fluss.annotation.Internal;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A consumer of the TabletServer-wide KV memory pool.
 *
 * <p>The global manager owns admission across consumers, while each consumer tracks its own
 * acquired bytes. This prevents one consumer from releasing memory acquired by another and provides
 * an idempotent way to release all memory during cleanup.
 */
@Internal
@NotThreadSafe
abstract class KvMemoryConsumer {

    private final KvPreWriteBufferMemoryManager memoryManager;
    private long acquiredBytes;

    protected KvMemoryConsumer(KvPreWriteBufferMemoryManager memoryManager) {
        this.memoryManager = checkNotNull(memoryManager, "Memory manager must not be null.");
    }

    protected final boolean tryAcquireMemory(long bytes) {
        checkArgument(bytes >= 0, "The number of bytes to acquire must not be negative.");
        if (!memoryManager.tryReserve(bytes)) {
            return false;
        }
        acquiredBytes += bytes;
        return true;
    }

    protected final void freeMemory(long bytes) {
        checkArgument(bytes >= 0, "The number of bytes to free must not be negative.");
        checkState(
                bytes <= acquiredBytes,
                "Cannot free %s bytes when this consumer has only acquired %s bytes.",
                bytes,
                acquiredBytes);
        if (bytes > 0) {
            memoryManager.release(bytes);
            acquiredBytes -= bytes;
        }
    }

    protected final void freeAllMemory() {
        if (acquiredBytes > 0) {
            memoryManager.release(acquiredBytes);
            acquiredBytes = 0;
        }
    }

    protected final KvPreWriteBufferMemoryManager memoryManager() {
        return memoryManager;
    }
}
