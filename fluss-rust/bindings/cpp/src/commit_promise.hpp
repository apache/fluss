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

#pragma once

#include "fluss.hpp"

namespace fluss::ffi {
struct FfiConsumerOffsetsResult;

class CommitPromise {
   public:
    using Convert = OffsetCommitResult (*)(FfiConsumerOffsetsResult&);
    explicit CommitPromise(Convert convert) : convert_(convert) {}
    std::future<OffsetCommitResult> future() { return promise_.get_future(); }
    void resolve(FfiConsumerOffsetsResult& value) noexcept {
        try {
            promise_.set_value(convert_(value));
        } catch (...) {
            promise_.set_exception(std::current_exception());
        }
    }

   private:
    Convert convert_;
    std::promise<OffsetCommitResult> promise_;
};
}  // namespace fluss::ffi
