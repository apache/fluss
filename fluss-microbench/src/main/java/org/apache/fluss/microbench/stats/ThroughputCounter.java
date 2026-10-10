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

package org.apache.fluss.microbench.stats;

import java.util.concurrent.atomic.LongAdder;

/** Thread-safe throughput counter tracking operations and bytes. */
public class ThroughputCounter {
    private final LongAdder ops = new LongAdder();
    private final LongAdder bytes = new LongAdder();

    public void record(long bytesWritten) {
        ops.increment();
        bytes.add(bytesWritten);
    }

    public long totalOps() {
        return ops.sum();
    }

    public long totalBytes() {
        return bytes.sum();
    }

    public double opsPerSecond(long elapsedNanos) {
        if (elapsedNanos <= 0) {
            return 0;
        }
        return ops.sum() * 1_000_000_000.0 / elapsedNanos;
    }

    public double bytesPerSecond(long elapsedNanos) {
        if (elapsedNanos <= 0) {
            return 0;
        }
        return bytes.sum() * 1_000_000_000.0 / elapsedNanos;
    }
}
