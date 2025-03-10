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

package com.alibaba.fluss.server.log;

/** Enum to define log fetch isolation levels. */
public enum FetchIsolation {
    // Indicates that fetching can be up to the end of the log, like follower can fetch record whose
    // offset bigger that the leader high watermark.
    LOG_END,
    // Indicates that fetching should be up to the high watermark, which is the offset
    // up to which all replicas have caught up.
    HIGH_WATERMARK;

    public static FetchIsolation of(boolean isFromFollower) {
        if (isFromFollower) {
            return LOG_END;
        } else {
            return HIGH_WATERMARK;
        }
    }
}
