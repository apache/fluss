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

#include <gtest/gtest.h>

#include <cstdlib>
#include <new>

#include "write_callback.hpp"

namespace {
// Fault injection is confined to this test thread; production has no allocation hook.
thread_local bool fail_next_allocation = false;
}  // namespace

void* operator new(std::size_t size) {
    if (std::exchange(fail_next_allocation, false)) {
        throw std::bad_alloc();
    }
    if (void* value = std::malloc(size ? size : 1)) {
        return value;
    }
    throw std::bad_alloc();
}

void operator delete(void* value) noexcept { std::free(value); }
void operator delete(void* value, std::size_t) noexcept { std::free(value); }

TEST(WriteCallbackBridgeTest, ErrorTextAllocationFailureStillCompletesAndReleasesCapture) {
    const std::string message(4096, 'x');  // Exceeds small-string capacity.
    auto lifetime = std::make_shared<int>(42);
    std::weak_ptr<int> weak = lifetime;
    fluss::Result observed;
    int calls = 0;
    fluss::ffi::WriteCallback callback(
        [&, owned = std::move(lifetime)](const fluss::WriteCompletion& notification) {
            const auto& result = notification.result;
            ++calls;
            observed = result;
        });
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(1, 0);
    ASSERT_TRUE(callback.Reserve(capacity).Ok());
    const rust::Str text(message);
    fail_next_allocation = true;
    callback.Complete(fluss::ErrorCode::DELETION_DISABLED_EXCEPTION, text);
    const bool allocation_was_attempted = !std::exchange(fail_next_allocation, false);

    EXPECT_TRUE(allocation_was_attempted);
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(observed.error_code, fluss::ErrorCode::DELETION_DISABLED_EXCEPTION);
    EXPECT_TRUE(observed.error_message.empty());
    EXPECT_TRUE(weak.expired());
    ASSERT_TRUE(capacity->Acquire().Ok());
    capacity->Release();
}
