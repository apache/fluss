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

package com.alibaba.fluss.client.admin;

import com.alibaba.fluss.annotation.PublicEvolving;

/**
 * Offset spec. Used for list offsets request.
 *
 * @since 0.2
 */
@PublicEvolving
public abstract class OffsetSpec {
    public static final int LIST_EARLIEST_OFFSET = 0;
    public static final int LIST_LATEST_OFFSET = 1;
    public static final int LIST_OFFSET_FROM_TIMESTAMP = 2;

    /** Earliest offset spec. */
    public static class EarliestSpec extends OffsetSpec {}

    /** latest offset spec. */
    public static class LatestSpec extends OffsetSpec {}

    /** timestamp offset spec. */
    public static class TimestampSpec extends OffsetSpec {
        private final long timestamp;

        public TimestampSpec(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}
