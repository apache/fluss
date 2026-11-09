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

#include <condition_variable>
#include <chrono>
#include <cstdio>
#include <exception>
#include <limits>
#include <mutex>

#include "fluss.hpp"
#include "rust/cxx.h"

namespace fluss {
namespace ffi {

/// Per-writer admission control; independent of Rust buffer memory and ACK completion.
class WriteCallbackCapacity {
   public:
    /// `wait_timeout_ms` is the connection's client.writer.buffer.wait-timeout, used as
    /// the shared budget for capacity and buffer waits. UINT64_MAX means block until a slot frees.
    WriteCallbackCapacity(size_t max_pending_operations, uint64_t wait_timeout_ms)
        : max_pending_(max_pending_operations), wait_timeout_ms_(wait_timeout_ms) {}

    Result Acquire() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (pending_ == max_pending_) {
            auto has_slot = [&] { return pending_ < max_pending_; };
            // Fail fast from within a callback to avoid stalling the shared workers on
            // their own capacity; a zero budget also rejects immediately.
            if (in_callback_ || wait_timeout_ms_ == 0) {
                return {ErrorCode::CLIENT_ERROR, "Write callback capacity is full"};
            }
            if (IsUnbounded()) {
                available_.wait(lock, has_slot);
            } else if (!available_.wait_for(lock, std::chrono::milliseconds(wait_timeout_ms_),
                                            has_slot)) {
                return {ErrorCode::CLIENT_ERROR, "Timed out waiting for write callback capacity"};
            }
        }
        ++pending_;
        return {};
    }

    void Release() noexcept {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            --pending_;
        }
        // notify_all: Acquire() waiters and the AwaitAll() waiter share this condvar,
        // so waking only one risks waking AwaitAll() (still pending) while an Acquire()
        // waiter keeps sleeping despite the freed slot.
        available_.notify_all();
    }

    /// True only while this thread executes a user callback or destroys its captures.
    static bool InCallback() { return in_callback_; }

    /// Wait until every reserved callback and its captures have finished.
    /// Flush rejects callback reentry before starting any write flush.
    void AwaitAll() {
        std::unique_lock<std::mutex> lock(mutex_);
        available_.wait(lock, [&] { return pending_ == 0; });
    }

    /// Milliseconds left in the client.writer.buffer.wait-timeout budget since `start`, so
    /// the buffer-backpressure wait plus the capacity reservation stay within one timeout
    /// (Kafka max.block.ms style). Floored at 0 (0 = fail fast). Returns -1 when the timeout
    /// is unbounded, letting the buffer wait fall back to the writer's configured timeout.
    int64_t RemainingBudgetMs(std::chrono::steady_clock::time_point start) const {
        if (IsUnbounded()) {
            return -1;
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);
        int64_t remaining = static_cast<int64_t>(wait_timeout_ms_) - elapsed.count();
        return remaining > 0 ? remaining : 0;
    }

   private:
    friend class WriteCallback;
    inline static thread_local bool in_callback_ = false;
    bool IsUnbounded() const { return wait_timeout_ms_ == std::numeric_limits<uint64_t>::max(); }
    const size_t max_pending_;
    const uint64_t wait_timeout_ms_;
    std::mutex mutex_;
    std::condition_variable available_;
    size_t pending_ = 0;
};

/// Owns a callback transferred to Rust. Access is exclusive, never concurrent.
class WriteCallback {
   public:
    explicit WriteCallback(fluss::WriteCallback callback) : callback_(std::move(callback)) {}

    WriteCallback(const WriteCallback&) = delete;
    WriteCallback& operator=(const WriteCallback&) = delete;

    /// Reserve before entering Rust. Destruction also returns capacity on submission failure.
    Result Reserve(std::shared_ptr<WriteCallbackCapacity> capacity) {
        if (!capacity) {
            return {ErrorCode::CLIENT_ERROR, "Writer not available"};
        }
        auto result = capacity->Acquire();
        if (result.Ok()) {
            reservation_.capacity = std::move(capacity);
        }
        return result;
    }

    /// Invoke once, containing all C++ exceptions on this side of the FFI boundary.
    void Complete(int32_t error_code, rust::Str error_message) noexcept {
        // Release captures before the reservation, even if this wrapper outlives Complete().
        Reservation reservation{std::move(reservation_.capacity)};
        CallbackScope scope;
        // Moving std::function alone need not empty the source. Swap with an
        // empty function so captures are released even if the callback throws.
        fluss::WriteCallback callback;
        callback.swap(callback_);
        Result result;
        result.error_code = error_code;
        try {
            result.error_message = std::string(error_message);
        } catch (...) {
            // Error text is best-effort; allocation failure must not skip completion.
            std::fprintf(stderr, "Fluss write callback could not copy error text (code %d)\n",
                         error_code);
        }
        try {
            callback(WriteCompletion{std::move(result)});
        } catch (const std::exception& e) {
            std::fprintf(stderr, "Fluss write callback threw an exception: %s\n", e.what());
        } catch (...) {
            std::fprintf(stderr, "Fluss write callback threw an unknown exception\n");
        }
    }

   private:
    struct Reservation {
        std::shared_ptr<WriteCallbackCapacity> capacity;
        ~Reservation() {
            if (capacity) {
                capacity->Release();
            }
        }
    };

    struct CallbackScope {
        bool previous = std::exchange(WriteCallbackCapacity::in_callback_, true);
        ~CallbackScope() { WriteCallbackCapacity::in_callback_ = previous; }
    };

    // Member order keeps captures alive until invocation, but not past capacity release.
    Reservation reservation_;
    fluss::WriteCallback callback_;
};

}  // namespace ffi
}  // namespace fluss
