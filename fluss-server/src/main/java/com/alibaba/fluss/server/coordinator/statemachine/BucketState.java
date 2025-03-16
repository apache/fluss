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

package com.alibaba.fluss.server.coordinator.statemachine;

import com.alibaba.fluss.metadata.TableBucket;

import java.util.EnumSet;
import java.util.Set;

/** An enum for the state of {@link TableBucket}. */
public enum BucketState implements BaseState<BucketState> {
    NonExistentBucket {
        @Override
        public Set<BucketState> getValidPreviousStates() {
            return EnumSet.of(OfflineBucket);
        }
    },
    OnlineBucket {
        @Override
        public Set<BucketState> getValidPreviousStates() {
            return EnumSet.of(NewBucket, OnlineBucket, OfflineBucket);
        }
    },
    NewBucket {
        @Override
        public Set<BucketState> getValidPreviousStates() {
            return EnumSet.of(NonExistentBucket);
        }
    },
    OfflineBucket {
        @Override
        public Set<BucketState> getValidPreviousStates() {
            return EnumSet.of(NewBucket, OnlineBucket, OfflineBucket);
        }
    }
}
