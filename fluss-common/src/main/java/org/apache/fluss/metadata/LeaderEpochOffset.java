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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.Internal;

import java.util.Objects;

/** A leader epoch and its associated log boundary. */
@Internal
public final class LeaderEpochOffset {
    private final int epoch;
    private final long offset;

    public LeaderEpochOffset(int epoch, long offset) {
        this.epoch = epoch;
        this.offset = offset;
    }

    public int epoch() {
        return epoch;
    }

    public long offset() {
        return offset;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LeaderEpochOffset)) {
            return false;
        }
        LeaderEpochOffset that = (LeaderEpochOffset) other;
        return epoch == that.epoch && offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, offset);
    }

    @Override
    public String toString() {
        return "LeaderEpochOffset(" + epoch + ", " + offset + ")";
    }
}
